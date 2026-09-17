/*
 * Copyright (c) 2017 Lubos Dolezel
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/ioctl.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/queue.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <mach/message.h>
#include <mach/mach.h>
#include <mach/mach_port.h>
#include <darling/emulation/linux_premigration/ext/for-libkqueue.h>
#include <darlingserver/rpc-supplement.h>

#include "private.h"

#ifndef LINUX_CLOCK_MONOTONIC
#define LINUX_CLOCK_MONOTONIC 1
#endif

static void machport_knote_log(const char *fmt, ...);

static int
machport_msgcount(mach_port_name_t name)
{
	mach_port_status_t status;
	mach_msg_type_number_t count = MACH_PORT_RECEIVE_STATUS_COUNT;
	kern_return_t kr;

	memset(&status, 0, sizeof(status));
	kr = mach_port_get_attributes(mach_task_self(), name,
	    MACH_PORT_RECEIVE_STATUS, (mach_port_info_t)&status, &count);
	if (kr != KERN_SUCCESS) {
		return -1;
	}
	return (int)status.mps_msgcount;
}

static void
machport_drain_timer(struct knote *kn)
{
	uint64_t ticks;

	if (kn->data.pfd < 0) {
		return;
	}
	while (read(kn->data.pfd, &ticks, sizeof(ticks)) == (ssize_t)sizeof(ticks)) {
	}
}

int
evfilt_machport_copyout(struct kevent64_s *dst, struct knote *src, void *ptr)
{
    struct epoll_event * const ev = (struct epoll_event *) ptr;
	dserver_kqchan_call_notification_t notification;
	dserver_kqchan_call_mach_port_read_t call;
	dserver_kqchan_reply_mach_port_read_t reply = {0};
	int rv;

    epoll_event_dump(ev);
    kevent_int_to_64(&src->kev, dst);

	machport_drain_timer(src);

	if (src->kdata.kn_dupfd < 0) {
		int nmsg = machport_msgcount((mach_port_name_t)src->kev.ident);
		if (nmsg > 0) {
			machport_knote_log("copyout msgcount=%d port=%llu",
			    nmsg, (unsigned long long)src->kev.ident);
			return 0;
		}
		dst->filter = EVFILT_DROP;
		return 0;
	}

	// first, read the notification (nonblocking: timerfd may have woken us)
	rv = recv(src->kdata.kn_dupfd, &notification, sizeof(notification), MSG_DONTWAIT);
	if (rv < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EBADF) {
			int nmsg = machport_msgcount((mach_port_name_t)src->kev.ident);
			if (nmsg > 0) {
				dbg_printf("evfilt_machport_copyout() timer/msgcount=%d port=%llu",
				    nmsg, (unsigned long long)src->kev.ident);
				return 0;
			}
			dst->filter = EVFILT_DROP;
			return 0;
		}
		dbg_printf("evfilt_machport_copyout() reading notification failed: %d (%s)", errno, strerror(errno));
		return -1;
	}

	if (notification.header.number != dserver_kqchan_msgnum_notification) {
		dbg_puts("evfilt_machport_copyout() read invalid notification");
		return -1;
	}

	// next, request the data
	call.header.number = dserver_kqchan_msgnum_mach_port_read;
	call.header.pid = getpid();
	call.header.tid = THREAD_ID;
	call.default_buffer = (uint64_t)&src->kn_extra_buffer[0];
	call.default_buffer_size = sizeof(src->kn_extra_buffer);
	rv = send(src->kdata.kn_dupfd, &call, sizeof(call), 0);
	if (rv < 0) {
		dbg_printf("evfilt_machport_copyout() sending request failed: %d (%s)", errno, strerror(errno));
		return -1;
	}

	// now, read the reply
	rv = recv(src->kdata.kn_dupfd, &reply, sizeof(reply), 0);
	if (rv < 0) {
		dbg_printf("evfilt_machport_copyout() reading reply failed: %d (%s)", errno, strerror(errno));
		return -1;
	}

	if (reply.header.number != dserver_kqchan_msgnum_mach_port_read) {
		dbg_puts("evfilt_machport_copyout() read invalid reply");
		return -1;
	}

	if (reply.header.code == 0xdead) {
		int nmsg = machport_msgcount((mach_port_name_t)src->kev.ident);
		if (nmsg > 0) {
			return 0;
		}
		dst->filter = EVFILT_DROP;
		return 0;
	}

	if (reply.header.code != 0) {
		// FIXME: the returned code is actually a Linux code (but strerror is provided by Darwin libc here)
		dbg_printf("evfilt_machport_copyout() server indicated failure: %d (%s)", -reply.header.code, strerror(-reply.header.code));
		return -1;
	}

	if (reply.kev.flags != 0)
		dst->flags = reply.kev.flags;
	dst->data = reply.kev.data;
	dst->ext[0] = reply.kev.ext[0];
	dst->ext[1] = reply.kev.ext[1];
	dst->fflags = reply.kev.fflags;

	// TODO: we need to properly support kevent_qos with regards to the data_out argument;
	//       this is what's supposed to be passed in as the default buffer, not a buffer of our own.

    return (0);
}

static void
machport_knote_log(const char *fmt, ...)
{
	static FILE *fp;
	va_list ap;

	if (fp == NULL) {
		fp = fopen("/tmp/machport-knote.log", "a");
		if (fp == NULL) {
			return;
		}
		setvbuf(fp, NULL, _IOLBF, 0);
	}
	fprintf(fp, "pid=%d ", (int)getpid());
	va_start(ap, fmt);
	vfprintf(fp, fmt, ap);
	va_end(ap);
	fputc('\n', fp);
}

int
evfilt_machport_knote_create(struct filter *filt, struct knote *kn)
{
    struct epoll_event ev;
    struct itimerspec its;
    int port = kn->kev.ident;
    int tfd;
    int epfd;

    kn->data.pfd = -1;
    kn->kdata.kn_dupfd = -1;
    kn->kn_epollfd = filter_epfd(filt);
    epfd = kn->kn_epollfd;

    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.ptr = kn;

	/* Arm the msgcount poller BEFORE kqchan. Chromium MachPortRendezvous
	 * HandleRequest is driven by EVFILT_MACHPORT; kqchan open on a live
	 * unserved check-in port can block or fail and previously skipped
	 * this wakeup entirely. */
	tfd = timerfd_create(LINUX_CLOCK_MONOTONIC, 0);
	if (tfd < 0) {
		machport_knote_log("timerfd_create port=%d errno=%d", port, errno);
	} else {
		fcntl(tfd, F_SETFD, FD_CLOEXEC);
		fcntl(tfd, F_SETFL, O_NONBLOCK);
		memset(&its, 0, sizeof(its));
		its.it_interval.tv_nsec = 25000000;
		its.it_value.tv_nsec = 25000000;
		if (timerfd_settime(tfd, 0, &its, NULL) < 0) {
			machport_knote_log("timerfd_settime port=%d tfd=%d errno=%d", port, tfd, errno);
			__close_for_kqueue(tfd);
			tfd = -1;
		} else if (epoll_ctl(epfd, EPOLL_CTL_ADD, tfd, &ev) < 0) {
			machport_knote_log("epoll_ctl timerfd port=%d tfd=%d epfd=%d errno=%d",
			    port, tfd, epfd, errno);
			__close_for_kqueue(tfd);
			tfd = -1;
		} else {
			kn->data.pfd = tfd;
		}
	}

	machport_knote_log("create port=%d epfd=%d timerfd=%d", port, epfd, kn->data.pfd);
	fprintf(stderr, "MACHPORT_CREATE port=%d epfd=%d timerfd=%d\n",
	    port, epfd, kn->data.pfd);
	fflush(stderr);

	if (kn->data.pfd < 0) {
		return (-1);
	}
    return 0;
}

int
evfilt_machport_knote_modify(struct filter *filt, struct knote *kn, 
        const struct kevent64_s *kev)
{
	dserver_kqchan_call_mach_port_modify_t call;
	dserver_kqchan_reply_mach_port_modify_t reply = {0};
	int rv;

	if (kn->kdata.kn_dupfd < 0) {
		return 0;
	}

	call.header.number = dserver_kqchan_msgnum_mach_port_modify;
	call.header.pid = getpid();
	call.header.tid = THREAD_ID;
	call.receive_buffer = kev->ext[0];
	call.receive_buffer_size = kev->ext[1];
	call.saved_filter_flags = kev->fflags;

	rv = send(kn->kdata.kn_dupfd, &call, sizeof(call), 0);
	if (rv < 0) {
		dbg_printf("evfilt_machport_knote_modify send failed: %d (%s)", errno, strerror(errno));
		return -1;
	}

	rv = recv(kn->kdata.kn_dupfd, &reply, sizeof(reply), 0);
	if (rv < 0) {
		dbg_printf("evfilt_machport_knote_modify recv failed: %d (%s)", errno, strerror(errno));
		return -1;
	}

	if (reply.header.number != dserver_kqchan_msgnum_mach_port_modify) {
		dbg_puts("evfilt_machport_knote_modify invalid reply");
		return -1;
	}

	if (reply.header.code != 0) {
		// FIXME: same as in copyout: Linux code but Darwin strerror
		dbg_printf("evfilt_machport_knote_modify call failed: %d (%s)", -reply.header.code, strerror(-reply.header.code));
		return -1;
	}

    return 0;
}

int
evfilt_machport_knote_delete(struct filter *filt, struct knote *kn)
{
    if ((kn->kev.flags & EV_DISABLE) == 0) {
        if (kn->kdata.kn_dupfd >= 0 &&
            epoll_ctl(kn->kn_epollfd, EPOLL_CTL_DEL, kn->kdata.kn_dupfd, NULL) < 0) {
            dbg_perror("epoll_ctl(2)");
            return (-1);
        }
        if (kn->data.pfd >= 0) {
            (void)epoll_ctl(kn->kn_epollfd, EPOLL_CTL_DEL, kn->data.pfd, NULL);
        }
    }

	if (kn->data.pfd >= 0) {
		(void) __close_for_kqueue(kn->data.pfd);
		kn->data.pfd = -1;
	}
	if (kn->kdata.kn_dupfd >= 0) {
		(void) __close_for_kqueue(kn->kdata.kn_dupfd);
		kn->kdata.kn_dupfd = -1;
	}
	return 0;
}

int
evfilt_machport_knote_enable(struct filter *filt, struct knote *kn)
{
    struct epoll_event ev;

    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.ptr = kn;

	dbg_printf("enabling machport knote with ID=%llu for events %d", kn->kev.ident, ev.events);

	if (kn->kdata.kn_dupfd >= 0 &&
	    epoll_ctl(kn->kn_epollfd, EPOLL_CTL_ADD, kn->kdata.kn_dupfd, &ev) < 0) {
		dbg_perror("epoll_ctl(2)");
		return (-1);
	}
	if (kn->data.pfd >= 0 &&
	    epoll_ctl(kn->kn_epollfd, EPOLL_CTL_ADD, kn->data.pfd, &ev) < 0) {
		dbg_perror("epoll_ctl(2) timerfd");
	}
	return (0);
}

int
evfilt_machport_knote_disable(struct filter *filt, struct knote *kn)
{
	dbg_printf("disable machport knote with ID=%llu", kn->kev.ident);
	if (kn->kdata.kn_dupfd >= 0 &&
	    epoll_ctl(kn->kn_epollfd, EPOLL_CTL_DEL, kn->kdata.kn_dupfd, NULL) < 0) {
		dbg_perror("epoll_ctl(2)");
		return (-1);
	}
	if (kn->data.pfd >= 0) {
		(void)epoll_ctl(kn->kn_epollfd, EPOLL_CTL_DEL, kn->data.pfd, NULL);
	}
	return (0);
}

const struct filter evfilt_machport = {
    EVFILT_MACHPORT,
    NULL,
    NULL,
    evfilt_machport_copyout,
    evfilt_machport_knote_create,
    evfilt_machport_knote_modify,
    evfilt_machport_knote_delete,
    evfilt_machport_knote_enable,
    evfilt_machport_knote_disable,         
};
