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

#ifdef DARLING
#include <crt_externs.h>
#endif

#ifndef LINUX_CLOCK_MONOTONIC
#define LINUX_CLOCK_MONOTONIC 1
#endif

__attribute__((used)) const char machport_drain_hot_v1[] =
        "machport_drain_hot_v1";
__attribute__((used)) const char machport_kqchan_notify_v1[] =
        "machport_kqchan_notify_v1";
__attribute__((used)) const char machport_queued_drain_v1[] =
        "machport_queued_drain_v1";
__attribute__((used)) const char machport_inbound_flush_v1[] =
        "machport_inbound_flush_v1";
__attribute__((used)) const char machport_notify_ack_v1[] =
        "machport_notify_ack_v1";
__attribute__((used)) const char machport_sameport_rearm_v1[] =
        "machport_sameport_rearm_v1";
__attribute__((used)) const char machport_helper_timerfd_skip_v1[] =
        "machport_helper_timerfd_skip_v1";
__attribute__((used)) const char machport_gpu_notif0_drop_v1[] =
        "machport_gpu_notif0_drop_v1";
__attribute__((used)) const char machport_gpu_cmdline_v1[] =
        "machport_gpu_cmdline_v1";
__attribute__((used)) const char machport_gpu_detect_v1[] =
        "machport_gpu_detect_v1";
__attribute__((used)) const char machport_net_remain_ack_v1[] =
        "machport_net_remain_ack_v1";
__attribute__((used)) const char machport_handshake_drain_bind_v1[] =
        "handshake_drain_bind_v1";
__attribute__((used)) const char machport_handshake_bind_burst_v1[] =
        "handshake_bind_burst_v1";
__attribute__((used)) const char machport_nested_timeout0_v1[] =
        "channelmac_nested_timeout0_v1";
__attribute__((used)) const char machport_nested_dispatch_bind_v1[] =
        "channelmac_nested_timeout0_v1 dispatch Bind";
__attribute__((used)) const char machport_nested_comet_frame_v1[] =
        "channelmac_nested_timeout0_v1 comet frame";
__attribute__((used)) const char machport_nested_storage_watcher_v1[] =
        "channelmac_nested_timeout0_v1 storage watcher";
__attribute__((used)) const char machport_handshake_drain_recv_v1[] =
        "handshake_drain_recv_v1";
__attribute__((used)) const char machport_handshake_drain_recv_browser_v1[] =
        "handshake_drain_recv_v1 browser Channel";
__attribute__((used)) const char machport_handshake_drain_recv_helper_v1[] =
        "handshake_drain_recv_v1 helper Channel";
__attribute__((used)) const char machport_no_nest1_recapture_v1[] =
        "channelmac_no_nest1_recapture_v1";
__attribute__((used)) const char machport_dispatch_queue_v1[] =
        "channelmac_dispatch_queue_v1";
__attribute__((used)) const char machport_dispatch_no_drain20_v1[] =
        "channelmac_dispatch_no_drain20_v1";
__attribute__((used)) const char machport_dispatch_34_ahead_v1[] =
        "channelmac_dispatch_34_ahead_v1";
__attribute__((used)) const char machport_dispatch_34_nest0_v1[] =
        "channelmac_dispatch_34_ahead_v1 nest=0 msgid=34";
__attribute__((used)) const char machport_dispatch_34_skip_nest1_v1[] =
        "channelmac_dispatch_34_ahead_v1 skip nest=1";
__attribute__((used)) const char machport_dispatch_34_after_offer_v1[] =
        "channelmac_dispatch_34_ahead_v1 nest=0 after Offer";
__attribute__((used)) const char machport_dispatch_skip20_after_offer_v1[] =
        "channelmac_dispatch_34_ahead_v1 skip inbound 20 after Offer";
__attribute__((used)) const char machport_dispatch_34_perport_v1[] =
        "channelmac_dispatch_34_ahead_v1 per-port stash";
__attribute__((used)) const char machport_dispatch_skip_restored_v1[] =
        "channelmac_dispatch_no_drain20_v1 skip restored port";
__attribute__((used)) const char machport_dispatch_34_browser_rot_v1[] =
        "channelmac_dispatch_34_ahead_v1 skip leftover 20 after Bind";
__attribute__((used)) const char machport_dispatch_skip20_after_bind_reverted_v1[] =
        "skip leftover 20 after Bind reverted";
__attribute__((used)) const char machport_leftover20_after_stash34_v1[] =
        "leftover 20 after Bind continue after stash34";
__attribute__((used)) const char machport_pong_transmit_nest0_v1[] =
        "pong_transmit_nest0_v1";
__attribute__((used)) const char machport_unlock_around_callback_v1[] =
        "channelmac_unlock_around_callback_v1";
__attribute__((used)) const char machport_unlock_around_nested_pong_v1[] =
        "channelmac_unlock_around_callback_v1 nested proto=4 send";
__attribute__((used)) const char machport_unlock_around_ping_only_v1[] =
        "channelmac_unlock_around_callback_v1 nest=1 Ping only";
__attribute__((used)) const char machport_skip_hs_ping_reverted_v1[] =
        "pong_transmit_nest0_v1 skip leftover hs Ping reverted";
__attribute__((used)) const char machport_nodelink_ping_v1[] =
        "nodelink_ping_copyout_v1";
__attribute__((used)) const char machport_ping_gap_v1[] =
        "channelmac_ping_gap_v1";
__attribute__((used)) const char machport_parcel_dump_v1[] =
        "channelmac_parcel_dump_v1";
__attribute__((used)) const char machport_samechan_bind_seq4_v1[] =
        "samechan_bind_seq4_v1";
__attribute__((used)) const char machport_handle_order_v1[] =
        "handle_order_v1";
__attribute__((used)) const char machport_channelmac_bound_v1[] =
        "channelmac_bound_v1";
__attribute__((used)) const char machport_desc12_copyout_v1[] =
        "desc12_copyout_v1";
__attribute__((used)) const char machport_helper_handle_ping_v1[] =
        "helper_handle_ping_v1";
__attribute__((used)) const char machport_helper_pong_send_v1[] =
        "helper_pong_send_v1";
__attribute__((used)) const char machport_helper_getrouter23_v1[] =
        "helper_getrouter23_v1";
__attribute__((used)) const char machport_helper_nodelink_routers_v1[] =
        "helper_nodelink_routers_v1";
__attribute__((used)) const char machport_helper_nodelink_sublinks_v1[] =
        "helper_nodelink_sublinks_v1";
__attribute__((used)) const char machport_helper_34_vs_ping_v1[] =
        "helper_34_vs_ping_v1";
__attribute__((used)) const char machport_helper_childproc_sl_v1[] =
        "helper_childproc_sl_v1";
__attribute__((used)) const char machport_helper_real_ping_v1[] =
        "helper_real_ping_v1";
__attribute__((used)) const char machport_helper_sl1424_name_v1[] =
        "helper_sl1424_name_v1";
__attribute__((used)) const char machport_invite_accept_trap_v1[] =
        "invite_accept_trap_v1";

static void machport_knote_log(const char *fmt, ...);
static int machport_kqchan_open(struct knote *kn);
static void machport_watch_try_port(mach_port_t port);
static int machport_is_network(void);
static int machport_is_storage(void);
static int machport_is_gpu_process(void);
static int machport_is_helper(void);
static void machport_invite_trap_dump(const char *why, mach_port_t port);
static int machport_hs_has(mach_port_t port);
static int machport_page_mapped(const void *p);
static int machport_maps_rw(const void *p);
static int machport_stash34_has(mach_port_t port);
static void machport_hex_n(const unsigned char *p, unsigned n, char *out,
    unsigned cap);
static int machport_childproc_bound;

#ifndef LINUX_SYS_read
#define LINUX_SYS_read 0
#define LINUX_SYS_close 3
#define LINUX_SYS_openat 257
#define LINUX_AT_FDCWD (-100)
#define LINUX_O_RDONLY 0
#define LINUX_O_CLOEXEC 02000000
#endif

/*
 * GPU helper: argv0 is Comet Helper / Comet Helper (GPU), and
 * Mach-O argv plus host /proc/self/cmdline carry gpu-process.
 * Darwin open(/proc/self/cmdline) is not the host cmdline
 * (code189 miss). Linux openat is. Do not cache a miss.
 */
static int
machport_token_is_gpu(const char *s)
{
	if (s == NULL || s[0] == '\0') {
		return 0;
	}
	if (strcmp(s, "--type=gpu-process") == 0) {
		return 1;
	}
	if (strstr(s, "Helper (GPU)") != NULL) {
		return 1;
	}
	return 0;
}

static int
machport_buf_has_gpu(const char *buf, ssize_t n, const char **hit)
{
	ssize_t i;

	if (buf == NULL || n <= 0) {
		return 0;
	}
	for (i = 0; i < n; ) {
		if (machport_token_is_gpu(buf + i)) {
			if (hit != NULL) {
				*hit = buf + i;
			}
			return 1;
		}
		i += (ssize_t)strlen(buf + i) + 1;
	}
	return 0;
}

static int
machport_linux_cmdline(char *buf, ssize_t cap)
{
	int fd;
	long n;

	/*
	 * Raw Linux openat: Darwin open(/proc/self/cmdline) is not
	 * the host execve argv (code189). No O_CLOEXEC — that bit is
	 * not the Linux value when mixed with Darwin fcntl.h.
	 */
	fd = __linux_syscall(LINUX_SYS_openat, (long)LINUX_AT_FDCWD,
	    (long)(uintptr_t)"/proc/self/cmdline", (long)LINUX_O_RDONLY,
	    0L, 0L, 0L);
	if (fd < 0) {
		return fd;
	}
	n = __linux_syscall(LINUX_SYS_read, (long)fd, (long)(uintptr_t)buf,
	    (long)(cap - 1), 0L, 0L, 0L);
	(void)__linux_syscall(LINUX_SYS_close, (long)fd, 0L, 0L, 0L, 0L, 0L);
	if (n <= 0) {
		return (n < 0) ? (int)n : -1;
	}
	buf[n] = '\0';
	return (int)n;
}

/*
 * Same-port 1ns re-arm is for Chrome_IOThread ChannelMac in the
 * browser (Ping seq 4 behind sl=2 seq 0–7). Child helpers
 * (--type=gpu-process / utility) ImmediateCrash PA 0x9105218 when
 * that re-arm drains their own Channel during cage setup.
 */
static int
machport_is_helper(void)
{
	static int cached = -1;
	int argc, i;
	char **argv;

	if (cached >= 0) {
		return cached > 0;
	}
#ifdef DARLING
	argc = *_NSGetArgc();
	argv = *_NSGetArgv();
	if (argv == NULL) {
		return 0;
	}
	for (i = 0; i < argc; i++) {
		if (argv[i] != NULL && strncmp(argv[i], "--type=", 7) == 0) {
			cached = 1;
			return 1;
		}
	}
	cached = 0;
#endif
	return 0;
}

/*
 * Stack-only: no BSS/DATA cache. Same NSGetArgv walk is_helper
 * already uses (that path runs in this copyout). Exact token
 * --type=gpu-process, not browser --disable-gpu-process-crash-limit.
 * Linux cmdline is fallback (dserver host-cmdline).
 */
static int
machport_is_gpu_process(void)
{
	char linux_buf[4096];
	const char *hit = NULL;
	int linux_n;
#ifdef DARLING
	int argc;
	int i;
	char **argv;
#endif

	(void)machport_gpu_cmdline_v1;
	(void)machport_gpu_detect_v1;
#ifdef DARLING
	argc = *_NSGetArgc();
	argv = *_NSGetArgv();
	if (argv != NULL && argc > 0 && argc < 256) {
		for (i = 0; i < argc; i++) {
			if (argv[i] != NULL && machport_token_is_gpu(argv[i])) {
				return 1;
			}
		}
	}
#endif
	linux_n = machport_linux_cmdline(linux_buf, sizeof(linux_buf));
	if (linux_n > 0 && machport_buf_has_gpu(linux_buf, linux_n, &hit)) {
		return 1;
	}
	return 0;
}

/*
 * Same cage heuristic as dserver procHasPaCage. Linux openat so
 * Darling fopen(/proc/self/maps) cannot miss. No BSS cache.
 */
static int
machport_self_has_pa_cage(void)
{
	char buf[8192];
	char line[256];
	int fd;
	long n;
	unsigned long start;
	unsigned long end;
	char perm[8];
	int li;

	fd = __linux_syscall(LINUX_SYS_openat, (long)LINUX_AT_FDCWD,
	    (long)(uintptr_t)"/proc/self/maps", (long)LINUX_O_RDONLY,
	    0L, 0L, 0L);
	if (fd < 0) {
		return 0;
	}
	li = 0;
	for (;;) {
		long i;

		n = __linux_syscall(LINUX_SYS_read, (long)fd,
		    (long)(uintptr_t)buf, (long)sizeof(buf), 0L, 0L, 0L);
		if (n <= 0) {
			break;
		}
		for (i = 0; i < n; i++) {
			if (buf[i] == '\n' || li == (int)sizeof(line) - 1) {
				line[li] = '\0';
				li = 0;
				perm[0] = '\0';
				start = 0;
				end = 0;
				if (sscanf(line, "%lx-%lx %7s", &start, &end, perm) == 3 &&
				    perm[0] == '-' && perm[1] == '-' && perm[2] == '-' &&
				    (end - start) >= (1UL << 30) &&
				    (start % (1UL << 34)) == 0) {
					(void)__linux_syscall(LINUX_SYS_close,
					    (long)fd, 0L, 0L, 0L, 0L, 0L);
					return 1;
				}
				continue;
			}
			line[li++] = buf[i];
		}
	}
	(void)__linux_syscall(LINUX_SYS_close, (long)fd, 0L, 0L, 0L, 0L, 0L);
	return 0;
}

static int
machport_is_network(void)
{
	char linux_buf[4096];
	int linux_n;
#ifdef DARLING
	int argc;
	int i;
	char **argv;
#endif

	(void)machport_net_remain_ack_v1;
#ifdef DARLING
	argc = *_NSGetArgc();
	argv = *_NSGetArgv();
	if (argv != NULL && argc > 0 && argc < 256) {
		for (i = 0; i < argc; i++) {
			if (argv[i] != NULL &&
			    strstr(argv[i], "network.mojom.NetworkService") != NULL) {
				return 1;
			}
		}
	}
#endif
	linux_n = machport_linux_cmdline(linux_buf, sizeof(linux_buf));
	if (linux_n > 0) {
		ssize_t i;

		for (i = 0; i < linux_n; ) {
			if (strstr(linux_buf + i, "network.mojom.NetworkService") != NULL) {
				return 1;
			}
			i += (ssize_t)strlen(linux_buf + i) + 1;
		}
	}
	return 0;
}

/*
 * Storage ChannelMac is the proven OnMachMessageReceived path
 * (MessagePumpKqueue ProcessEvents → watcher()->OnMachMessageReceived).
 * Capture that watcher so network can use the same slot / this.
 */
static int
machport_is_storage(void)
{
	char linux_buf[4096];
	int linux_n;
#ifdef DARLING
	int argc;
	int i;
	char **argv;

	argc = *_NSGetArgc();
	argv = *_NSGetArgv();
	if (argv != NULL && argc > 0 && argc < 256) {
		for (i = 0; i < argc; i++) {
			if (argv[i] != NULL &&
			    strstr(argv[i], "storage.mojom.StorageService") != NULL) {
				return 1;
			}
		}
	}
#endif
	linux_n = machport_linux_cmdline(linux_buf, sizeof(linux_buf));
	if (linux_n > 0) {
		ssize_t i;

		for (i = 0; i < linux_n; ) {
			if (strstr(linux_buf + i,
			    "storage.mojom.StorageService") != NULL) {
				return 1;
			}
			i += (ssize_t)strlen(linux_buf + i) + 1;
		}
	}
	return 0;
}

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

/*
 * Keep EVFILT_MACHPORT level-triggered while real kmsgs remain.
 * A 25ms timerfd left ChildProcessHost Ping (seq 4 on sublink 2)
 * 262ms behind msgid 34; the decaying Y→X LocalRouterLink then
 * dropped AcceptParcel and no reply copyin'd on 52/56.
 * Empty wakeups stay EVFILT_DROP (not delivered) so MachPortRendezvous
 * does not SIGTRAP on a vacant poll.
 */
static void
machport_rearm(struct knote *kn, int queued)
{
	struct itimerspec its;

	if (kn->data.pfd < 0) {
		return;
	}
	memset(&its, 0, sizeof(its));
	if (machport_is_helper()) {
		/*
		 * Helpers skip 1ns on both kqchan and timerfd-only knotes.
		 * The kqchan-only skip left GPU MACH_RCV_MSG ports on a
		 * 1ns+1ms interval during PA cage setup.
		 */
		if (kn->kdata.kn_dupfd < 0) {
			its.it_value.tv_nsec = 25000000;
			its.it_interval.tv_nsec = 25000000;
		}
	} else if (kn->kdata.kn_dupfd >= 0) {
		/*
		 * Browser ChannelMac is one timeout-0 mach_msg per kevent.
		 * 1ns one-shot on THIS port after a real delivery so
		 * remaining sl=2 msgid-20 seq 1–7 (Ping seq 4) drain.
		 */
		if (queued > 0) {
			its.it_value.tv_nsec = 1;
		}
	} else if (queued > 0) {
		its.it_value.tv_nsec = 1;
		its.it_interval.tv_nsec = 1000000;
	} else {
		its.it_value.tv_nsec = 25000000;
		its.it_interval.tv_nsec = 25000000;
	}
	(void)timerfd_settime(kn->data.pfd, 0, &its, NULL);
}

int
evfilt_machport_queued(struct knote *kn)
{
	if (kn == NULL) {
		return 0;
	}
	(void)machport_inbound_flush_v1;
	return machport_msgcount((mach_port_name_t)kn->kev.ident);
}

/*
 * XNU filt_machportprocess: EVFILT_MACHPORT with MACH_RCV_MSG copies
 * one kmsg into kn_ext[0] and returns the mach_msg result in fflags.
 * Timerfd-only knotes never opened kqchan, so copyout used to report
 * msgcount without receiving. Chrome HandleRequest then saw a live
 * event with an empty buffer, left the mqueue stuck, and GPU/network
 * helpers timed out / _exit (Z) after the browser's iokit lookup.
 */
static int
machport_copyout_receive(struct kevent64_s *dst, struct knote *src)
{
	mach_port_name_t port = (mach_port_name_t)src->kev.ident;
	mach_msg_option_t option;
	mach_msg_header_t *hdr;
	mach_msg_size_t size;
	kern_return_t kr;
	int nmsg;

	(void)machport_sameport_rearm_v1;
	(void)machport_helper_timerfd_skip_v1;
	(void)machport_nested_timeout0_v1;
	nmsg = machport_msgcount(port);
	/*
	 * code245: kernel mqueue only. Fake nmsg=1 when hs_q is
	 * occupied (code246 hs_pop-all) emptied leftover 20s
	 * before leftover-20 after Bind, leaving 34-only.
	 * Restored Connect 20s still hs_pop on a real timeout-0.
	 */
	if (nmsg <= 0) {
		machport_rearm(src, 0);
		machport_knote_log("copyout drop port=%u nmsg=%d",
		    (unsigned)port, nmsg);
		dst->filter = EVFILT_DROP;
		return 0;
	}

	option = src->kev.fflags & (MACH_RCV_MSG | MACH_RCV_LARGE |
	    MACH_RCV_LARGE_IDENTITY | MACH_RCV_TRAILER_MASK |
	    MACH_RCV_VOUCHER | MACH_MSG_STRICT_REPLY);
	option |= MACH_RCV_TIMEOUT;

	if ((option & MACH_RCV_MSG) == 0) {
		int is_gpu;

		/*
		 * Peek → this notify is the copyout that actually runs.
		 * Log on stderr first (knote_log of the 4-arg check line
		 * never appeared even when this TEXT was mapped).
		 */
		is_gpu = machport_is_gpu_process();
		fprintf(stderr,
		    "copyout notify-check gpu=%d port=%u nmsg=%d pid=%d\n",
		    is_gpu, (unsigned)port, nmsg, (int)getpid());
		fflush(stderr);
		machport_knote_log(
		    "copyout notify-check gpu=%d port=%u nmsg=%d",
		    is_gpu, (unsigned)port, nmsg);
		if (is_gpu && machport_self_has_pa_cage() == 0) {
			(void)machport_gpu_notif0_drop_v1;
			fprintf(stderr,
			    "copyout drop notif=0 gpu timerfd port=%u nmsg=%d pid=%d\n",
			    (unsigned)port, nmsg, (int)getpid());
			fflush(stderr);
			machport_knote_log(
			    "copyout drop notif=0 gpu timerfd port=%u nmsg=%d",
			    (unsigned)port, nmsg);
			machport_rearm(src, 0);
			dst->filter = EVFILT_DROP;
			return 0;
		}
		/*
		 * ChannelMac notify-only: data=port, fflags=
		 * MACH_RCV_TOO_LARGE. data=nmsg + fflags=0 made the
		 * browser skip timeout-0 mach_msg (0 copyout of
		 * helper Bypass/Accept; EnsureConnected 15s).
		 * Do not fake-fire empty MACHPORT.
		 */
		src->kn_flags |= KNFL_MACHPORT_DELIVERED;
		dst->data = port;
		dst->fflags = MACH_RCV_TOO_LARGE;
		/*
		 * Handshake drain Bind at post: after Offer/mjhs
		 * drain, Bind is nmsg=1. EV_DISPATCH would disable
		 * before ChannelMac timeout-0, so Bind waited for
		 * enable after Ping (~9ms). Keep armed for nmsg>=2
		 * (Offer+Bind) and for nmsg=1 once this knote has
		 * already delivered (Bind-alone). Helper-skip-1ns.
		 * Do not fake-fire empty MACHPORT.
		 */
		if (!machport_is_gpu_process() &&
		    (nmsg >= 2 ||
		     (nmsg >= 1 &&
		      (src->kn_flags & KNFL_MACHPORT_DELIVERED) != 0)) &&
		    (machport_is_network() || machport_is_storage() ||
		     !machport_is_helper()) &&
		    !(machport_is_helper() && !machport_childproc_bound)) {
			(void)machport_handshake_drain_bind_v1;
			(void)machport_handshake_bind_burst_v1;
			dst->flags &= ~EV_DISPATCH;
			src->kn_flags |= KNFL_MACHPORT_HS_DRAIN;
			if (!machport_is_helper()) {
				(void)machport_handshake_drain_recv_browser_v1;
				machport_knote_log(
				    "handshake_drain_recv_v1 browser Channel copyout port=%u nmsg=%d keep-armed",
				    (unsigned)port, nmsg);
				fprintf(stderr,
				    "handshake_drain_recv_v1 browser Channel copyout pid=%d port=%u nmsg=%d keep-armed\n",
				    (int)getpid(), (unsigned)port, nmsg);
				fflush(stderr);
			} else {
				(void)machport_handshake_drain_recv_helper_v1;
				machport_knote_log(
				    "handshake_drain_recv_v1 helper Channel copyout port=%u nmsg=%d keep-armed",
				    (unsigned)port, nmsg);
				fprintf(stderr,
				    "handshake_drain_recv_v1 helper Channel copyout pid=%d port=%u nmsg=%d keep-armed\n",
				    (int)getpid(), (unsigned)port, nmsg);
				fflush(stderr);
			}
		} else if (machport_is_helper() && !machport_childproc_bound &&
		    nmsg >= 1) {
			fprintf(stderr,
			    "invite_accept_trap_v1 skip keep-armed until BindReceiver pid=%d port=%u nmsg=%d bound=0\n",
			    (int)getpid(), (unsigned)port, nmsg);
			fflush(stderr);
			machport_knote_log(
			    "invite_accept_trap_v1 skip keep-armed until BindReceiver port=%u nmsg=%d",
			    (unsigned)port, nmsg);
		}
		machport_rearm(src, nmsg);
		machport_knote_log("copyout notify port=%u nmsg=%d",
		    (unsigned)port, nmsg);
		return 0;
	}

	hdr = (mach_msg_header_t *)(uintptr_t)src->kev.ext[0];
	size = (mach_msg_size_t)src->kev.ext[1];
	if (hdr == NULL || size < sizeof(mach_msg_header_t)) {
		dst->fflags = MACH_RCV_TOO_LARGE;
		dst->ext[1] = 0;
		dst->data = (option & MACH_RCV_LARGE_IDENTITY) ? port : MACH_PORT_NULL;
		machport_rearm(src, nmsg);
		machport_knote_log("copyout rcv port=%u nmsg=%d no-buffer",
		    (unsigned)port, nmsg);
		return 0;
	}

	kr = mach_msg(hdr, option, 0, size, port, 0, MACH_PORT_NULL);
	dst->fflags = kr;
	dst->ext[0] = (uint64_t)(uintptr_t)hdr;
	if (kr == MACH_MSG_SUCCESS) {
		int remain;

		src->kn_flags |= KNFL_MACHPORT_DELIVERED;
		dst->ext[1] = hdr->msgh_size;
		dst->data = MACH_PORT_NULL;
		remain = machport_msgcount(port);
		machport_rearm(src, remain > 0 ? remain : 0);
	} else if (kr == MACH_RCV_TOO_LARGE) {
		int remain;

		src->kn_flags |= KNFL_MACHPORT_DELIVERED;
		dst->ext[1] = hdr->msgh_size;
		dst->data = (option & MACH_RCV_LARGE_IDENTITY) ? port : MACH_PORT_NULL;
		remain = machport_msgcount(port);
		if (remain < 1) {
			remain = nmsg;
		}
		machport_rearm(src, remain > 0 ? remain : 0);
	} else if (kr == MACH_RCV_TIMED_OUT) {
		machport_rearm(src, 0);
		dst->filter = EVFILT_DROP;
		return 0;
	} else {
		dst->data = MACH_PORT_NULL;
		machport_rearm(src, nmsg);
	}
	machport_knote_log("copyout rcv port=%u nmsg=%d kr=0x%x size=%u id=%d",
	    (unsigned)port, nmsg, (unsigned)kr,
	    (unsigned)((kr == MACH_MSG_SUCCESS || kr == MACH_RCV_TOO_LARGE)
		? hdr->msgh_size : 0),
	    (kr == MACH_MSG_SUCCESS) ? hdr->msgh_id : 0);
	return 0;
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
		return machport_copyout_receive(dst, src);
	}

	/*
	 * ChannelMac is notify-only. Waiting on kqchan fill serialized one
	 * parcel per kernelAsync (~6–19ms) while Ping sat behind msgid 34.
	 * Peek the mqueue immediately; ack mach_port_read without blocking.
	 */
	if ((src->kev.fflags & MACH_RCV_MSG) == 0) {
		int fd = src->kdata.kn_dupfd;
		unsigned char buf[sizeof(dserver_kqchan_reply_mach_port_read_t)];
		int got_notif = 0;

		for (;;) {
			rv = recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
			if (rv < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EBADF) {
					break;
				}
				dbg_printf("evfilt_machport_copyout() notify drain failed: %d (%s)",
				    errno, strerror(errno));
				return -1;
			}
			if (rv < (int)sizeof(dserver_kqchan_replyhdr_t)) {
				break;
			}
			{
				dserver_kqchan_msgnum_t num =
				    ((dserver_kqchan_replyhdr_t *)buf)->number;
				if (num == dserver_kqchan_msgnum_notification) {
					got_notif = 1;
				} else if (num == dserver_kqchan_msgnum_mach_port_read) {
					src->kn_flags &= ~KNFL_KQCHAN_NEED_REPLY;
				}
			}
		}
		{
			int nmsg = machport_msgcount((mach_port_name_t)src->kev.ident);
			int ack;

			/*
			 * Browser: ACK on leftover kmsgs so Ping seq 1–7
			 * drain when the timerfd raced the datagram.
			 * Network helper: ACK leftover for Offer+Bind
			 * (nmsg>=2) and Bind-alone (nmsg=1 after this
			 * knote already delivered). GPU stays
			 * notif-only (PA). Helper-skip-1ns.
			 */
			ack = got_notif || (nmsg > 0 &&
			    (!machport_is_helper() ||
			     ((machport_is_network() || machport_is_storage()) &&
			      !machport_is_gpu_process() &&
			      (nmsg >= 2 ||
			       (src->kn_flags & KNFL_MACHPORT_DELIVERED) != 0))));
			if (ack && (src->kn_flags & KNFL_KQCHAN_NEED_REPLY) == 0) {
				dserver_kqchan_callhdr_t ackmsg;

				/*
				 * ChannelMac is notify-only. mach_port_read fill
				 * impersonates Chrome_IOThread and races timeout-0
				 * mach_msg (network nmsg=21 after 7 peeks). ACK
				 * restores kqchan notify without touching the mqueue.
				 */
				memset(&ackmsg, 0, sizeof(ackmsg));
				ackmsg.number = dserver_kqchan_msgnum_mach_port_notify_ack;
				ackmsg.pid = getpid();
				ackmsg.tid = THREAD_ID;
				(void)send(fd, &ackmsg, sizeof(ackmsg), MSG_DONTWAIT);
			}
			machport_knote_log("kqchan notify-peek port=%u notif=%d nmsg=%d need_reply=%d",
			    (unsigned)src->kev.ident, got_notif, nmsg,
			    (src->kn_flags & KNFL_KQCHAN_NEED_REPLY) ? 1 : 0);
			/*
			 * Do not detect/drop here. code191 logged peek then
			 * copyout notify — drop belongs in copyout_receive.
			 */
		}
		return machport_copyout_receive(dst, src);
	}

	// first, read the notification (nonblocking: timerfd may have woken us)
	rv = recv(src->kdata.kn_dupfd, &notification, sizeof(notification), MSG_DONTWAIT);
	if (rv < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EBADF) {
			return machport_copyout_receive(dst, src);
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
		machport_knote_log("kqchan read dead port=%u fallback-timer",
		    (unsigned)src->kev.ident);
		return machport_copyout_receive(dst, src);
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

	machport_knote_log("kqchan copyout port=%u fflags=0x%x data=%d",
	    (unsigned)src->kev.ident, (unsigned)dst->fflags, (int)dst->data);

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

static int
machport_kqchan_open(struct knote *kn)
{
	int ret;
	int fd = -1;
	struct epoll_event ev;
	mach_port_name_t port = (mach_port_name_t)kn->kev.ident;

	/*
	 * ChannelMac WatchMachReceivePort is notify-only (no MACH_RCV_MSG).
	 * kqchan posts when dtape enqueues a kmsg, so Chrome_IOThread's
	 * kevent wakes immediately and timeout-0 mach_msg can copyout Ping
	 * before LocalRouterLink Deactivate. MACH_RCV_MSG knotes (wakeup
	 * port, MachPortRendezvous HandleRequest) stay timerfd-only:
	 * kqchan attach on an unserved check-in right blocked/failed.
	 */
	if ((kn->kev.fflags & MACH_RCV_MSG) != 0) {
		return 0;
	}

	ret = _dserver_rpc_kqchan_mach_port_open_4libkqueue(port,
	    (void *)(uintptr_t)kn->kev.ext[0], kn->kev.ext[1],
	    kn->kev.fflags, &fd);
	if (ret < 0 || fd < 0) {
		machport_knote_log("kqchan open fail port=%u ret=%d fd=%d",
		    (unsigned)port, ret, fd);
		return -1;
	}

	fcntl(fd, F_SETFD, FD_CLOEXEC);
	fcntl(fd, F_SETFL, O_NONBLOCK);
	memset(&ev, 0, sizeof(ev));
	ev.events = EPOLLIN;
	ev.data.ptr = kn;
	if (epoll_ctl(kn->kn_epollfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
		machport_knote_log("kqchan epoll_ctl fail port=%u fd=%d errno=%d",
		    (unsigned)port, fd, errno);
		__close_for_kqueue(fd);
		return -1;
	}
	kn->kdata.kn_dupfd = fd;
	machport_knote_log("kqchan open port=%u fd=%d", (unsigned)port, fd);
	return 0;
}

int
evfilt_machport_knote_create(struct filter *filt, struct knote *kn)
{
    struct epoll_event ev;
    struct itimerspec its;
    int port = kn->kev.ident;
    int tfd;
    int epfd;
    int have_kqchan;

    kn->data.pfd = -1;
    kn->kdata.kn_dupfd = -1;
    kn->kn_epollfd = filter_epfd(filt);
    epfd = kn->kn_epollfd;

    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.ptr = kn;

	have_kqchan = (machport_kqchan_open(kn) == 0 && kn->kdata.kn_dupfd >= 0);

	/*
	 * Always attach a timerfd. With kqchan it stays disarmed while the
	 * port is empty (kqchan wakes enqueue) and one-shots when copyout
	 * still sees kmsgs — ChannelMac does not loop.
	 * Without kqchan (MACH_RCV_MSG) keep the 25ms empty poll.
	 */
	tfd = timerfd_create(LINUX_CLOCK_MONOTONIC, 0);
	if (tfd < 0) {
		machport_knote_log("timerfd_create port=%d errno=%d", port, errno);
	} else {
		fcntl(tfd, F_SETFD, FD_CLOEXEC);
		fcntl(tfd, F_SETFL, O_NONBLOCK);
		memset(&its, 0, sizeof(its));
		if (!have_kqchan) {
			its.it_interval.tv_nsec = 25000000;
			its.it_value.tv_nsec = 25000000;
		}
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
			machport_rearm(kn, machport_msgcount((mach_port_name_t)port));
		}
	}

	machport_knote_log("create port=%d epfd=%d timerfd=%d kqchan=%d",
	    port, epfd, kn->data.pfd, kn->kdata.kn_dupfd);
	fprintf(stderr, "MACHPORT_CREATE pid=%d port=%d epfd=%d timerfd=%d kqchan=%d\n",
	    (int)getpid(), port, epfd, kn->data.pfd, kn->kdata.kn_dupfd);
	fflush(stderr);

	/*
	 * ChannelMac WatchMachReceivePort is notify-only. Capture the
	 * MachPortWatcher (ChannelMac MI subobject) from this stack so
	 * the send interpose can OnMachMessageReceived before Ping.
	 */
	if ((kn->kev.fflags & MACH_RCV_MSG) == 0 &&
	    !machport_is_gpu_process() &&
	    (machport_is_network() || machport_is_storage())) {
		(void)machport_nested_storage_watcher_v1;
		machport_watch_try_port((mach_port_t)port);
	}

	if (kn->data.pfd < 0 && kn->kdata.kn_dupfd < 0) {
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

	if (kev->fflags != 0 || kev->ext[0] != 0 || kev->ext[1] != 0) {
		kn->kev.fflags = kev->fflags;
		kn->kev.ext[0] = kev->ext[0];
		kn->kev.ext[1] = kev->ext[1];
	}

	if (kn->kdata.kn_dupfd < 0) {
		int nmsg = machport_msgcount((mach_port_name_t)kn->kev.ident);

		if (nmsg > 0) {
			machport_rearm(kn, nmsg);
		}
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

	/*
	 * EV_CLEAR/oneshot re-enable on THIS port only. ChannelMac does
	 * not loop; remaining kmsgs after seq 0 need a 1ns poke once the
	 * knote has already delivered. Do not fake-fire empty MACHPORT.
	 */
	if ((kn->kn_flags & KNFL_MACHPORT_DELIVERED) != 0) {
		int nmsg = machport_msgcount((mach_port_name_t)kn->kev.ident);

		if (nmsg > 0) {
			machport_rearm(kn, nmsg);
		}
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
	/*
	 * EV_DISPATCH disables after the first kevent. libdispatch
	 * EV_ENABLE's after ChannelMac's timeout-0 mach_msg. If THIS
	 * port still has kmsgs and the knote already delivered once,
	 * 1ns one-shot so seq 1–7 (Ping seq 4) wake without a global
	 * remain-notify storm.
	 */
	if ((kn->kn_flags & KNFL_MACHPORT_DELIVERED) != 0) {
		int nmsg = machport_msgcount((mach_port_name_t)kn->kev.ident);

		if (nmsg > 0) {
			machport_rearm(kn, nmsg);
			machport_knote_log("enable rearm port=%u nmsg=%d",
			    (unsigned)kn->kev.ident, nmsg);
			/*
			 * Network helper-skip-1ns: Bind sitting with
			 * Offer (nmsg>=2) or Bind-alone after this knote
			 * delivered (nmsg=1 msgid=22) ACKs leftover so
			 * remain latch copyouts msgid=22. Do not 1ns.
			 * GPU stays skipped.
			 */
			if (kn->kdata.kn_dupfd >= 0 &&
			    machport_is_network() &&
			    !machport_is_gpu_process() &&
			    (nmsg >= 2 ||
			     (nmsg >= 1 &&
			      (kn->kn_flags & KNFL_MACHPORT_DELIVERED) != 0))) {
				dserver_kqchan_callhdr_t ackmsg;

				memset(&ackmsg, 0, sizeof(ackmsg));
				ackmsg.number = dserver_kqchan_msgnum_mach_port_notify_ack;
				ackmsg.pid = getpid();
				ackmsg.tid = THREAD_ID;
				(void)send(kn->kdata.kn_dupfd, &ackmsg,
				    sizeof(ackmsg), MSG_DONTWAIT);
				machport_knote_log(
				    "machport_net_remain_ack_v1 enable port=%u nmsg=%d",
				    (unsigned)kn->kev.ident, nmsg);
			}
		}
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

/*
 * Reentrant same-thread Bind dispatch during ChannelMac send.
 * Kernel nested timeout-0 copyouts Bind, but OnMachMessageReceived
 * used to run on the bounce take ~11ms after Ping copyin, so RLS
 * stayed 0x0 at Ping. Interpose mach_msg (libc overlay; do not
 * touch libsystem_kernel) and invoke the watcher before the send
 * trap so ipcz can write RLS 0xfffffffe at Ping. Network only.
 * Never 1ns in gpu-process. Do not park Ping.
 */
extern mach_msg_return_t mach_msg_trap(mach_msg_header_t *msg,
    mach_msg_option_t option, mach_msg_size_t send_size,
    mach_msg_size_t rcv_size, mach_port_t rcv_name,
    mach_msg_timeout_t timeout, mach_port_t notify);

#define MACHPORT_MAP_MAX 96
#define MACHPORT_WATCH_MAX 8

struct machport_map_range {
	uintptr_t start;
	uintptr_t end;
	unsigned rx;
	unsigned rw;
	unsigned comet;
};

static struct machport_map_range machport_maps[MACHPORT_MAP_MAX];
static int machport_maps_n;

struct machport_watch {
	void *thisp;
	void *sub;
	void *chan;
	void (*fn)(void *, mach_port_t);
	mach_port_t port;
	long ot_top;
	int thunk;
};

static struct machport_watch machport_watches[MACHPORT_WATCH_MAX];
static int machport_watch_n;

/*
 * Last AcceptParcel router descriptor Chrome Handle'd (helper recv).
 * sl=17 seq=2 is SIMPLE Mach; routers live in the ipcz payload.
 */
static uint64_t machport_desc_sl;
static unsigned machport_desc_seq;
static unsigned machport_desc_complex;
static unsigned machport_desc_ndesc;
static uint32_t machport_desc_nhandles;
static uint32_t machport_desc_nrouters;
static uint32_t machport_desc_roff;
static uint64_t machport_desc_newsl;
static uint64_t machport_desc_decaying;
static uint64_t machport_desc_next_in;
static uint64_t machport_desc_dec_len;
static unsigned machport_desc_flags;
static uint32_t machport_desc_msz;
static uint64_t machport_desc_frag_buf;
static uint32_t machport_desc_frag_off;
static uint32_t machport_desc_frag_sz;
static uint32_t machport_desc_name0;
static uint32_t machport_desc_name1;
static uint32_t machport_desc_ch_type;
static uint32_t machport_desc_d_off;

#define MACHPORT_SL_MAX 64
static void *machport_sl_router[MACHPORT_SL_MAX + 1];
static void *machport_sl_link[MACHPORT_SL_MAX + 1];

struct machport_xfer {
	uint64_t recv_sl;
	unsigned seq;
	uint64_t new_sl;
	uint64_t decaying;
	uint64_t next_in;
	uint64_t dec_len;
	unsigned flags;
	uint32_t name0;
	uint32_t name1;
	uint32_t ch_type;
};
static struct machport_xfer machport_xfers[12];
static unsigned machport_xfer_n;

/*
 * ChannelMac::Write holds write_lock_ across MachMessageSendLocked.
 * Darling nest=1 OnMachMessageReceived runs inside that send. ChildProcess
 * Ping's Write(Pong) cannot take a non-recursive write_lock_. Darwin would
 * not nest receive inside Write — unlock around proto=4 Ping only.
 * Leftover AcceptParcels keep the lock (code245 NativeWidget 992).
 * Do not pass ChannelMac* as this (fn this=sub). Do not patch Comet.
 */
#define MACHPORT_MTX_SIG 0x4D555458ul
#define MACHPORT_MTX_SIG_FAST 0x4D55545Aul

struct machport_mtx_view {
	long sig;
	uint32_t lock;
	uint32_t mtxopts;
	int16_t prioceiling;
	int16_t priority;
	uint32_t pad;
	uint32_t m_tid[2];
};

static uint64_t
machport_mutex_owner(pthread_mutex_t *m)
{
	struct machport_mtx_view *v;
	uintptr_t tidp;

	v = (struct machport_mtx_view *)(void *)m;
	tidp = ((uintptr_t)v->m_tid + 7ul) & ~7ul;
	if (!machport_page_mapped((const void *)tidp)) {
		return 0;
	}
	return *(const uint64_t *)tidp;
}

static pthread_mutex_t *
machport_channelmac_held_lock(void *chan)
{
	unsigned off;
	uint64_t self = 0;
	uint64_t owner;

	if (chan == NULL || !machport_maps_rw(chan)) {
		return NULL;
	}
	(void)pthread_threadid_np(NULL, &self);
	if (self == 0) {
		return NULL;
	}
	for (off = 0x40; off < 0x400; off += 8) {
		void *p = (char *)chan + off;
		struct machport_mtx_view *v;
		unsigned long sig;

		if (!machport_page_mapped(p)) {
			continue;
		}
		v = (struct machport_mtx_view *)p;
		sig = (unsigned long)v->sig;
		if (sig != MACHPORT_MTX_SIG && sig != MACHPORT_MTX_SIG_FAST) {
			continue;
		}
		owner = machport_mutex_owner((pthread_mutex_t *)p);
		if (owner == self) {
			return (pthread_mutex_t *)p;
		}
	}
	return NULL;
}

static __thread pthread_mutex_t *machport_cb_unlock_lock;
static __thread int machport_leftover_got_ping;

static void
machport_unlock_for_ping(mach_port_t port)
{
	int i;
	pthread_mutex_t *lock;
	int ur = -1;
	void *chan = NULL;
	void *sub = NULL;

	(void)machport_unlock_around_callback_v1;
	(void)machport_unlock_around_ping_only_v1;
	if (machport_cb_unlock_lock != NULL) {
		return;
	}
	machport_leftover_got_ping = 1;
	lock = NULL;
	for (i = 0; i < machport_watch_n; i++) {
		if (machport_watches[i].port == port &&
		    machport_watches[i].chan != NULL &&
		    machport_watches[i].ot_top == -88) {
			chan = machport_watches[i].chan;
			sub = machport_watches[i].sub;
			lock = machport_channelmac_held_lock(chan);
			break;
		}
	}
	if (lock != NULL) {
		ur = pthread_mutex_unlock(lock);
		machport_cb_unlock_lock = lock;
		fprintf(stderr,
		    "channelmac_unlock_around_callback_v1 unlock pid=%d chan=%p lock=%p off=%ld ur=%d this=sub=%p proto=4 nest=1 Ping only\n",
		    (int)getpid(), chan, (void *)lock,
		    chan != NULL ? (long)((char *)lock - (char *)chan) : 0L,
		    ur, sub);
		fflush(stderr);
	} else {
		fprintf(stderr,
		    "channelmac_unlock_around_callback_v1 miss pid=%d chan=%p this=sub=%p proto=4 nest=1 Ping only\n",
		    (int)getpid(), chan, sub);
		fflush(stderr);
	}
}

static void
machport_call_onmach(struct machport_watch *w)
{
	pthread_mutex_t *lock;
	int lr = -1;

	(void)machport_unlock_around_callback_v1;
	(void)machport_unlock_around_ping_only_v1;
	machport_cb_unlock_lock = NULL;
	w->fn(w->sub, w->port);
	lock = machport_cb_unlock_lock;
	machport_cb_unlock_lock = NULL;
	if (lock != NULL) {
		lr = pthread_mutex_lock(lock);
		fprintf(stderr,
		    "channelmac_unlock_around_callback_v1 relock pid=%d lock=%p lr=%d proto=4 nest=1 Ping only\n",
		    (int)getpid(), (void *)lock, lr);
		fflush(stderr);
	}
}

static __thread int machport_in_dispatch;
static __thread int machport_ping_wait_send;
static __thread int machport_ping_wait_sends;
static __thread unsigned machport_ping_wait_port;
static __thread int machport_hping_seen;
static __thread int machport_h34_n;
static __thread uint64_t machport_h34_sl[8];
static __thread uint64_t machport_h34_new[8];
static __thread int machport_h34_send[8];
static __thread int machport_dispatch_stop_20;
static __thread int machport_dispatch_got_34;
static __thread int machport_after_offer_busy;
static __thread int machport_skip_hs20_until_34;
static __thread mach_port_t machport_restored_20_port;
static mach_msg_header_t *machport_offer_rcv;
static mach_msg_option_t machport_offer_rcv_opt;
static mach_msg_size_t machport_offer_rcv_size;
static mach_msg_size_t machport_offer_rcv_n;
static mach_port_t machport_offer_rcv_port;

#define MACHPORT_STASH34_MAX 8

struct machport_stash34_slot {
	int valid;
	mach_port_t port;
	mach_msg_return_t kr;
	mach_msg_size_t nbytes;
	unsigned char buf[8192];
};

static struct machport_stash34_slot machport_stash34[MACHPORT_STASH34_MAX];

#define MACHPORT_BIND_DONE_MAX 8

static mach_port_t machport_bind_done[MACHPORT_BIND_DONE_MAX];
static int machport_bind_done_n;

static int
machport_bind_done_has(mach_port_t port)
{
	int i;

	for (i = 0; i < machport_bind_done_n; i++) {
		if (machport_bind_done[i] == port) {
			return 1;
		}
	}
	return 0;
}

static void
machport_bind_done_set(mach_port_t port)
{
	if (port == MACH_PORT_NULL || machport_bind_done_has(port)) {
		return;
	}
	if (machport_bind_done_n >= MACHPORT_BIND_DONE_MAX) {
		return;
	}
	machport_bind_done[machport_bind_done_n++] = port;
}

#ifndef LINUX_SYS_mincore
#define LINUX_SYS_mincore 27
#endif

static int
machport_page_mapped(const void *p)
{
	unsigned char vec = 0;
	uintptr_t page;
	long r;

	if (p == NULL) {
		return 0;
	}
	page = (uintptr_t)p & ~(uintptr_t)0xfff;
	r = __linux_syscall(LINUX_SYS_mincore, (long)page, 4096L,
	    (long)(uintptr_t)&vec, 0L, 0L, 0L);
	return r == 0;
}

static int
machport_maps_refresh(void)
{
	char buf[4096];
	char line[1024];
	int fd;
	long n;
	int li = 0;
	int i;

	fd = (int)__linux_syscall(LINUX_SYS_openat, (long)LINUX_AT_FDCWD,
	    (long)(uintptr_t)"/proc/self/maps", (long)LINUX_O_RDONLY,
	    0L, 0L, 0L);
	if (fd < 0) {
		return 0;
	}
	machport_maps_n = 0;
	for (;;) {
		n = __linux_syscall(LINUX_SYS_read, (long)fd,
		    (long)(uintptr_t)buf, (long)sizeof(buf), 0L, 0L, 0L);
		if (n <= 0) {
			break;
		}
		for (i = 0; i < (int)n; i++) {
			if (buf[i] == '\n' || li >= (int)sizeof(line) - 1) {
				unsigned long start = 0, end = 0;
				char perms[8];
				struct machport_map_range *r;

				line[li] = '\0';
				li = 0;
				if (strstr(line, "Comet Helper") != NULL) {
					continue;
				}
				if (strstr(line, "Comet Framework") == NULL) {
					continue;
				}
				memset(perms, 0, sizeof(perms));
				if (sscanf(line, "%lx-%lx %7s", &start, &end,
				    perms) != 3 || end <= start) {
					continue;
				}
				if (machport_maps_n >= MACHPORT_MAP_MAX) {
					continue;
				}
				r = &machport_maps[machport_maps_n];
				r->start = (uintptr_t)start;
				r->end = (uintptr_t)end;
				r->rx = (perms[0] == 'r' && perms[2] == 'x');
				r->rw = (perms[0] == 'r' && perms[1] == 'w');
				r->comet = 1;
				machport_maps_n++;
				continue;
			}
			line[li++] = buf[i];
		}
	}
	(void)__linux_syscall(LINUX_SYS_close, (long)fd, 0L, 0L, 0L, 0L, 0L);
	return machport_maps_n;
}

/*
 * Code (r-x): ChannelMac methods. Do not require 8-byte alignment —
 * x86-64 return addresses are often misaligned (ret0 diagnostic).
 */
static int
machport_maps_code(const void *p)
{
	uintptr_t a = (uintptr_t)p;
	int i;

	if (p == NULL) {
		return 0;
	}
	for (i = 0; i < machport_maps_n; i++) {
		if (a >= machport_maps[i].start && a < machport_maps[i].end &&
		    machport_maps[i].rx) {
			return 1;
		}
	}
	return 0;
}

static int
machport_maps_rx(const void *p, int comet_only)
{
	(void)comet_only;
	if (p == NULL || ((uintptr_t)p & 7) != 0) {
		return 0;
	}
	return machport_maps_code(p);
}

static int
machport_in_comet(const void *p)
{
	uintptr_t a = (uintptr_t)p;
	int i;

	if (p == NULL) {
		return 0;
	}
	for (i = 0; i < machport_maps_n; i++) {
		if (a >= machport_maps[i].start && a < machport_maps[i].end) {
			return 1;
		}
	}
	return 0;
}

/*
 * Mach-O vtables live in __DATA_CONST (r--), not r-x TEXT.
 * code224 required r-x for vptr so comet_rx=0 never found ChannelMac.
 */
static int
machport_maps_vtab(const void *p)
{
	uintptr_t a = (uintptr_t)p;
	int i;

	if (p == NULL || (a & 7) != 0) {
		return 0;
	}
	for (i = 0; i < machport_maps_n; i++) {
		if (a >= machport_maps[i].start && a < machport_maps[i].end &&
		    !machport_maps[i].rw) {
			return 1;
		}
	}
	return 0;
}

static int
machport_maps_rw(const void *p)
{
	uintptr_t a = (uintptr_t)p;
	int i;

	if (p == NULL || (a & 7) != 0) {
		return 0;
	}
	if (!machport_page_mapped(p)) {
		return 0;
	}
	for (i = 0; i < machport_maps_n; i++) {
		if (a >= machport_maps[i].start && a < machport_maps[i].end &&
		    machport_maps[i].rx) {
			return 0;
		}
	}
	return 1;
}

static void
machport_log_addr_map(const char *tag, const void *p)
{
	char buf[4096];
	char line[1024];
	int fd;
	long n;
	int li = 0;
	int i;
	uintptr_t a = (uintptr_t)p;

	(void)machport_nested_comet_frame_v1;
	fd = (int)__linux_syscall(LINUX_SYS_openat, (long)LINUX_AT_FDCWD,
	    (long)(uintptr_t)"/proc/self/maps", (long)LINUX_O_RDONLY,
	    0L, 0L, 0L);
	if (fd < 0) {
		fprintf(stderr,
		    "channelmac_nested_timeout0_v1 ret0 map pid=%d %s=%p maps_open_fail\n",
		    (int)getpid(), tag, p);
		fflush(stderr);
		return;
	}
	for (;;) {
		n = __linux_syscall(LINUX_SYS_read, (long)fd,
		    (long)(uintptr_t)buf, (long)sizeof(buf), 0L, 0L, 0L);
		if (n <= 0) {
			break;
		}
		for (i = 0; i < (int)n; i++) {
			if (buf[i] == '\n' || li >= (int)sizeof(line) - 1) {
				unsigned long start = 0, end = 0;
				char perms[8];
				char *path;

				line[li] = '\0';
				li = 0;
				memset(perms, 0, sizeof(perms));
				if (sscanf(line, "%lx-%lx %7s", &start, &end,
				    perms) != 3) {
					continue;
				}
				if (a < start || a >= end) {
					continue;
				}
				path = strchr(line, '/');
				if (path == NULL) {
					path = strstr(line, "[");
				}
				fprintf(stderr,
				    "channelmac_nested_timeout0_v1 ret0 map pid=%d %s=%p range=%lx-%lx perms=%s path=%s\n",
				    (int)getpid(), tag, p, start, end, perms,
				    path != NULL ? path : "-");
				fflush(stderr);
				(void)__linux_syscall(LINUX_SYS_close, (long)fd,
				    0L, 0L, 0L, 0L, 0L);
				return;
			}
			line[li++] = buf[i];
		}
	}
	(void)__linux_syscall(LINUX_SYS_close, (long)fd, 0L, 0L, 0L, 0L, 0L);
	fprintf(stderr,
	    "channelmac_nested_timeout0_v1 ret0 map pid=%d %s=%p unmapped\n",
	    (int)getpid(), tag, p);
	fflush(stderr);
}

static int
machport_vtable_fn_count(void **vptr)
{
	int n = 0;

	while (n < 48 && machport_maps_code(vptr[n])) {
		n++;
	}
	return n;
}

static int
machport_rel_fn(void *vptr, int idx, void **fn_out)
{
	int32_t off;
	void *fn;

	if (vptr == NULL || idx < 0 || idx > 8) {
		return 0;
	}
	off = ((int32_t *)vptr)[idx];
	if (off == 0 || off == -1) {
		return 0;
	}
	fn = (char *)vptr + (intptr_t)off;
	if (!machport_maps_code(fn)) {
		return 0;
	}
	*fn_out = fn;
	return 1;
}

/*
 * Itanium thunk: add/sub/lea rdi then jmp. Slot 2 of a secondary
 * MachPortWatcher vtable should be this. The previous run's
 * fn=0x…b28ef0 was the final OnMachMessageReceived prologue
 * (push rbp / sub rsp,0x1c8) — calling that with the watcher
 * subobject as this reads the wrong receive_port_.
 */
static int
machport_fn_is_thunk(const void *fn)
{
	const unsigned char *p = (const unsigned char *)fn;

	if (fn == NULL || !machport_page_mapped(fn)) {
		return 0;
	}
	if (p[0] == 0x48 && p[1] == 0x83 &&
	    (p[2] == 0xc7 || p[2] == 0xef) &&
	    (p[4] == 0xe9 || p[4] == 0xeb)) {
		return 1;
	}
	if (p[0] == 0x48 && p[1] == 0x8d && p[2] == 0x7f &&
	    (p[4] == 0xe9 || p[4] == 0xeb)) {
		return 1;
	}
	return 0;
}

/*
 * ChannelMac::OnMachMessageReceived / MachPortWatcher slot 2.
 * Storage capture fn8=554889e5 (push rbp; mov rbp,rsp). Network
 * ot=-424 picked a relative-offset that was not this prologue
 * (code232 send-interpose never called fn()).
 */
static int
machport_fn_is_onmach(const void *fn)
{
	const unsigned char *p = (const unsigned char *)fn;

	if (fn == NULL || !machport_page_mapped(fn)) {
		return 0;
	}
	if (p[0] == 0x55 && p[1] == 0x48 && p[2] == 0x89 && p[3] == 0xe5) {
		return 1;
	}
	return 0;
}

static void *
machport_channelmac_from_sub(void *sub)
{
	unsigned off;

	if (sub == NULL) {
		return NULL;
	}
	for (off = 8; off <= 0x200; off += 8) {
		void *cand = (char *)sub - off;
		void **vptr;
		int nfn;

		if (!machport_maps_rw(cand)) {
			continue;
		}
		vptr = *(void ***)cand;
		if (!machport_maps_vtab(vptr) && !machport_in_comet(vptr)) {
			continue;
		}
		nfn = machport_vtable_fn_count(vptr);
		if (nfn >= 8) {
			return cand;
		}
	}
	return NULL;
}

/*
 * Storage ProcessEvents: controller->watcher()->OnMachMessageReceived
 * is a virtual call on the MachPortWatcher subobject. The thunk
 * (or Itanium offset-to-top at vptr[-1]) yields ChannelMac*.
 */
static void
machport_resolve_channelmac(void *sub, void *fn, void **chan_out,
    long *ot_out, int *thunk_out)
{
	void **vptr;
	long ot;
	void *chan;

	*chan_out = NULL;
	*ot_out = 0;
	*thunk_out = machport_fn_is_thunk(fn);
	if (sub == NULL || !machport_maps_rw(sub)) {
		return;
	}
	vptr = *(void ***)sub;
	if (machport_maps_vtab(vptr) || machport_in_comet(vptr)) {
		ot = ((long *)vptr)[-1];
		if (ot <= 0 && ot >= -0x400 && (ot & 7) == 0) {
			chan = (char *)sub + ot;
			if (machport_maps_rw(chan)) {
				void **cv = *(void ***)chan;

				if (machport_vtable_fn_count(cv) >= 8) {
					*chan_out = chan;
					*ot_out = ot;
					return;
				}
			}
		}
	}
	chan = machport_channelmac_from_sub(sub);
	*chan_out = chan;
	if (chan != NULL) {
		*ot_out = (long)((char *)chan - (char *)sub);
	}
}

/*
 * ChannelMac : Channel, DestructionObserver, MachPortWatcher.
 * Watcher vptr is a secondary MI subobject. Vtable is r-- DATA_CONST;
 * slot 2 is OnMachMessageReceived (Itanium: two dtors + method).
 * Prefer a 3-slot that sits on a Channel (nfn>=8) so we do not
 * call DestructionObserver::WillDestroyCurrentMessageLoop.
 */
static int
machport_find_watcher(void *thisp, void **sub_out, void (**fn_out)(void *, mach_port_t))
{
	unsigned off;
	void *best_sub = NULL;
	void (*best_fn)(void *, mach_port_t) = NULL;
	int best_score = -1;

	if (!machport_maps_rw(thisp)) {
		return 0;
	}
	for (off = 0; off < 0x800; off += 8) {
		void **slot = (void **)((char *)thisp + off);
		void **vptr;
		void *fn = NULL;
		void *chan = NULL;
		long ot = 0;
		int thunk = 0;
		int nfn;
		int score;

		if (!machport_maps_rw(slot)) {
			break;
		}
		vptr = (void **)*slot;
		if (!machport_maps_vtab(vptr) && !machport_in_comet(vptr)) {
			continue;
		}
		nfn = 0;
		if (machport_maps_code(vptr[2])) {
			nfn = machport_vtable_fn_count(vptr);
			fn = vptr[2];
		} else if (machport_rel_fn(vptr, 2, &fn) ||
		    machport_rel_fn(vptr, 4, &fn)) {
			nfn = machport_vtable_fn_count(vptr);
		}
		if (fn == NULL) {
			continue;
		}
		/*
		 * MachPortWatcher is a 3-slot MI base on ChannelMac
		 * (ot=-88, this=sub). Network first-hit was ot=-424
		 * with a relative-offset that was not OnMach
		 * (code232). Require ot=-88. Do not pass ChannelMac*.
		 */
		machport_resolve_channelmac(slot, fn, &chan, &ot, &thunk);
		score = 0;
		if (ot == -88) {
			score += 8;
		}
		if (machport_fn_is_onmach(fn)) {
			score += 4;
		}
		if (nfn == 3) {
			score += 1;
		}
		if (off == 0) {
			score += 1;
		}
		if (score > best_score) {
			best_score = score;
			best_sub = slot;
			best_fn = (void (*)(void *, mach_port_t))fn;
		}
	}
	if (best_score >= 12 && best_sub != NULL && best_fn != NULL) {
		*sub_out = best_sub;
		*fn_out = best_fn;
		return 1;
	}
	return 0;
}

static void *
machport_scan_words(void **begin, void **end, int *cand_n, int *hit_n)
{
	void **p;
	void *cand;
	void *sub;
	void (*fn)(void *, mach_port_t);

	if (begin == NULL || end == NULL || begin >= end) {
		return NULL;
	}
	for (p = begin; p < end; p++) {
		cand = *p;
		if (!machport_maps_rw(cand)) {
			continue;
		}
		if (cand_n != NULL) {
			(*cand_n)++;
		}
		if (machport_find_watcher(cand, &sub, &fn)) {
			if (hit_n != NULL) {
				(*hit_n)++;
			}
			return cand;
		}
	}
	return NULL;
}

static void *
machport_caller_this(void)
{
	void *fp0;
	void *ret0;
	void *cand;
	void *reg[5];
	void *sub;
	void (*fn)(void *, mach_port_t);
	uintptr_t sp;
	int i;
	int cand_n = 0;
	int hit_n = 0;
	static int diag;

	ret0 = __builtin_return_address(0);
	fp0 = __builtin_frame_address(0);
	__asm__ volatile (
	    "movq %%rsp, %0\n"
	    "movq %%rbx, %1\n"
	    "movq %%r12, %2\n"
	    "movq %%r13, %3\n"
	    "movq %%r14, %4\n"
	    "movq %%r15, %5\n"
	    : "=r"(sp), "=r"(reg[0]), "=r"(reg[1]), "=r"(reg[2]),
	      "=r"(reg[3]), "=r"(reg[4]));
	if (diag < 6) {
		diag++;
		fprintf(stderr,
		    "channelmac_nested_timeout0_v1 capture diag pid=%d maps_n=%d ret0=%p comet_rx=%d fp0=%p sp=%p\n",
		    (int)getpid(), machport_maps_n, ret0,
		    machport_maps_code(ret0), fp0, (void *)sp);
		fflush(stderr);
		machport_log_addr_map("ret0", ret0);
	}
	for (i = 0; i < 5; i++) {
		cand = reg[i];
		if (machport_maps_rw(cand) &&
		    machport_find_watcher(cand, &sub, &fn)) {
			return cand;
		}
	}
	cand = machport_scan_words((void **)sp,
	    (void **)(sp + 0x4000), &cand_n, &hit_n);
	if (cand != NULL) {
		return cand;
	}
	if (fp0 != NULL) {
		cand = machport_scan_words((void **)fp0,
		    (void **)((char *)fp0 + 0x2000), &cand_n, &hit_n);
		if (cand != NULL) {
			return cand;
		}
	}
	if (diag <= 6) {
		fprintf(stderr,
		    "channelmac_nested_timeout0_v1 capture miss pid=%d cand=%d hit=%d\n",
		    (int)getpid(), cand_n, hit_n);
		fflush(stderr);
	}
	(void)ret0;
	return NULL;
}

static int
machport_watch_proven(void *sub, void *chan, long ot,
    void (*fn)(void *, mach_port_t))
{
	return fn != NULL && sub != NULL && chan != NULL && ot == -88;
}

static void
machport_watch_store(void *thisp, mach_port_t port)
{
	void *sub = NULL;
	void *chan = NULL;
	void (*fn)(void *, mach_port_t) = NULL;
	long ot_top = 0;
	int thunk = 0;
	int i;
	int new_ok;
	unsigned char fb[8];

	(void)machport_no_nest1_recapture_v1;
	if (machport_in_dispatch) {
		fprintf(stderr,
		    "channelmac_no_nest1_recapture_v1 skip store pid=%d port=%u nest=1\n",
		    (int)getpid(), (unsigned)port);
		fflush(stderr);
		return;
	}
	if (thisp == NULL) {
		return;
	}
	if (!machport_find_watcher(thisp, &sub, &fn)) {
		machport_knote_log(
		    "channelmac_nested_timeout0_v1 capture miss this=%p port=%u",
		    thisp, (unsigned)port);
		return;
	}
	machport_resolve_channelmac(sub, (void *)fn, &chan, &ot_top, &thunk);
	new_ok = machport_watch_proven(sub, chan, ot_top, fn);
	memset(fb, 0, sizeof(fb));
	if (fn != NULL && machport_page_mapped(fn)) {
		memcpy(fb, fn, sizeof(fb));
	}
	for (i = 0; i < machport_watch_n; i++) {
		if (machport_watches[i].thisp == thisp ||
		    (port != MACH_PORT_NULL &&
		     machport_watches[i].port == port)) {
			if (machport_watch_proven(machport_watches[i].sub,
			    machport_watches[i].chan,
			    machport_watches[i].ot_top,
			    machport_watches[i].fn) && !new_ok) {
				fprintf(stderr,
				    "channelmac_no_nest1_recapture_v1 keep proven pid=%d port=%u this=%p ot=%ld chan=%p skip this=%p ot=%ld chan=%p\n",
				    (int)getpid(), (unsigned)port,
				    machport_watches[i].thisp,
				    machport_watches[i].ot_top,
				    machport_watches[i].chan, thisp, ot_top,
				    chan);
				fflush(stderr);
				return;
			}
			machport_watches[i].thisp = thisp;
			machport_watches[i].sub = sub;
			machport_watches[i].chan = chan;
			machport_watches[i].fn = fn;
			machport_watches[i].ot_top = ot_top;
			machport_watches[i].thunk = thunk;
			if (port != MACH_PORT_NULL) {
				machport_watches[i].port = port;
			}
			return;
		}
	}
	if (machport_watch_n >= MACHPORT_WATCH_MAX) {
		return;
	}
	machport_watches[machport_watch_n].thisp = thisp;
	machport_watches[machport_watch_n].sub = sub;
	machport_watches[machport_watch_n].chan = chan;
	machport_watches[machport_watch_n].fn = fn;
	machport_watches[machport_watch_n].port = port;
	machport_watches[machport_watch_n].ot_top = ot_top;
	machport_watches[machport_watch_n].thunk = thunk;
	machport_watch_n++;
	(void)machport_nested_dispatch_bind_v1;
	fprintf(stderr,
	    "channelmac_nested_timeout0_v1 captured watcher pid=%d this=%p sub=%p chan=%p fn=%p thunk=%d ot=%ld port=%u fn8=%02x%02x%02x%02x%02x%02x%02x%02x net=%d stor=%d\n",
	    (int)getpid(), thisp, sub, chan, (void *)fn, thunk, ot_top,
	    (unsigned)port, fb[0], fb[1], fb[2], fb[3], fb[4], fb[5], fb[6],
	    fb[7], machport_is_network(), machport_is_storage());
	fflush(stderr);
	machport_knote_log(
	    "channelmac_nested_timeout0_v1 captured watcher this=%p sub=%p chan=%p fn=%p thunk=%d ot=%ld port=%u",
	    thisp, sub, chan, (void *)fn, thunk, ot_top, (unsigned)port);
	machport_invite_trap_dump("capture", port);
}

static void
machport_watch_try_port(mach_port_t port)
{
	void *thisp;

	machport_maps_refresh();
	thisp = machport_caller_this();
	if (thisp != NULL) {
		machport_watch_store(thisp, port);
	}
}

/*
 * Chrome LP64 copyout: 24-byte header, 12-byte port descriptors
 * (pad_end is KERNEL-only). Prefer that stride so COMPLEX Connect
 * and seq 0-3 handles match what Chrome reads. 16-byte kernel
 * stride is fallback only.
 */
static int
machport_ipcz_hdr_ok(const unsigned char *p, mach_msg_size_t avail)
{
	uint16_t sz = 0;

	if (p == NULL || avail < 8) {
		return 0;
	}
	memcpy(&sz, p, 2);
	if (sz == 16) {
		return 1;
	}
	if (avail > 8) {
		memcpy(&sz, p + 8, 2);
		if (sz == 16) {
			return 2;
		}
	}
	return 0;
}

static mach_msg_size_t
machport_chrome_ipcz_off(const mach_msg_header_t *msg, unsigned *ndesc_out)
{
	const unsigned char *base;
	mach_msg_size_t msz, ndesc, off;

	if (ndesc_out != NULL) {
		*ndesc_out = 0;
	}
	if (msg == NULL) {
		return 0;
	}
	base = (const unsigned char *)msg;
	msz = msg->msgh_size;
	ndesc = 0;
	if (msz >= 28 && (msg->msgh_bits & MACH_MSGH_BITS_COMPLEX) != 0) {
		memcpy(&ndesc, base + 24, 4);
		if (ndesc > 32) {
			ndesc = 0;
		}
	}
	if (ndesc_out != NULL) {
		*ndesc_out = (unsigned)ndesc;
	}
	off = 24u + 4u + ndesc * 12u;
	if (off < msz && machport_ipcz_hdr_ok(base + off, msz - off)) {
		return off;
	}
	off = 24u + 4u + ndesc * 16u;
	if (off < msz && machport_ipcz_hdr_ok(base + off, msz - off)) {
		return off;
	}
	off = 32u + 4u + ndesc * 16u;
	if (off < msz && machport_ipcz_hdr_ok(base + off, msz - off)) {
		return off;
	}
	return 24u + 4u + ndesc * 12u;
}

static void
machport_desc12_note(const mach_msg_header_t *msg, mach_port_t rcv_name,
    uint64_t sl, unsigned seq)
{
	const unsigned char *base;
	unsigned ndesc = 0;
	unsigned i;
	uint32_t bits = 0;
	uint32_t msz = 0;
	uint32_t name;
	mach_port_type_t ptype;
	unsigned disp;
	unsigned type;
	kern_return_t kr;
	static int nlog;

	(void)machport_desc12_copyout_v1;
	if (msg == NULL || nlog >= 64) {
		return;
	}
	base = (const unsigned char *)msg;
	memcpy(&bits, base, 4);
	memcpy(&msz, base + 4, 4);
	if (msz >= 28 && (bits & MACH_MSGH_BITS_COMPLEX) != 0) {
		memcpy(&ndesc, base + 24, 4);
		if (ndesc > 8) {
			ndesc = 8;
		}
	}
	nlog++;
	fprintf(stderr,
	    "desc12_copyout_v1 chrome pid=%d port=%u bits=0x%x size=%u complex=%u ndesc=%u hdr=%zu portdesc=%zu sl=%llu seq=%u\n",
	    (int)getpid(), (unsigned)rcv_name, bits, msz,
	    (bits & MACH_MSGH_BITS_COMPLEX) != 0, ndesc,
	    sizeof(mach_msg_header_t), sizeof(mach_msg_port_descriptor_t),
	    (unsigned long long)sl, seq);
	for (i = 0; i < ndesc; i++) {
		const unsigned char *d = base + 28 + i * 12u;

		if (28 + (i + 1) * 12u > msz) {
			break;
		}
		name = 0;
		memcpy(&name, d, 4);
		disp = d[10];
		type = d[11];
		ptype = 0;
		kr = mach_port_type(mach_task_self(), (mach_port_name_t)name,
		    &ptype);
		fprintf(stderr,
		    "desc12_copyout_v1 chrome i=%u name=0x%x disp=%u type=%u live_kr=0x%x ptype=0x%x send=%d recv=%d dead=%d\n",
		    i, name, disp, type, (unsigned)kr, (unsigned)ptype,
		    (ptype & MACH_PORT_TYPE_SEND) != 0,
		    (ptype & MACH_PORT_TYPE_RECEIVE) != 0,
		    (ptype & MACH_PORT_TYPE_DEAD_NAME) != 0);
	}
	fflush(stderr);
	machport_knote_log(
	    "desc12_copyout_v1 chrome port=%u ndesc=%u sl=%llu seq=%u complex=%u",
	    (unsigned)rcv_name, ndesc, (unsigned long long)sl, seq,
	    (bits & MACH_MSGH_BITS_COMPLEX) != 0);
}

static unsigned
machport_ipcz_msgid(const mach_msg_header_t *msg)
{
	const unsigned char *base;
	unsigned char msgid;
	mach_msg_size_t off, msg_size, avail;
	uint32_t num_bytes;
	uint16_t ipcz_size;

	if (msg == NULL || msg->msgh_id != (mach_msg_id_t)0x4d4f4a4fu) {
		return 0;
	}
	base = (const unsigned char *)msg;
	msg_size = msg->msgh_size;
	if (msg_size < 48 || msg_size > 8192) {
		return 0;
	}
	off = machport_chrome_ipcz_off(msg, NULL);
	if (off + 24 > msg_size) {
		return 0;
	}
	avail = msg_size - off;
	ipcz_size = 0;
	num_bytes = 0;
	memcpy(&ipcz_size, base + off, 2);
	memcpy(&num_bytes, base + off + 4, 4);
	if (ipcz_size != 16 && avail > 8) {
		memcpy(&ipcz_size, base + off + 8, 2);
		memcpy(&num_bytes, base + off + 12, 4);
		off += 8;
		avail -= 8;
	}
	(void)num_bytes;
	if (ipcz_size < 8 || avail < (mach_msg_size_t)ipcz_size + 4) {
		return 0;
	}
	msgid = base[off + ipcz_size + 2];
	return (unsigned)msgid;
}

static unsigned
machport_ipcz_proto20(const mach_msg_header_t *msg)
{
	const unsigned char *base;
	mach_msg_size_t off, msg_size, avail;
	uint32_t proto;
	uint16_t ipcz_size;

	if (machport_ipcz_msgid(msg) != 20) {
		return 0;
	}
	base = (const unsigned char *)msg;
	msg_size = msg->msgh_size;
	off = machport_chrome_ipcz_off(msg, NULL);
	if (off + 24 > msg_size) {
		return 0;
	}
	avail = msg_size - off;
	ipcz_size = 0;
	memcpy(&ipcz_size, base + off, 2);
	if (ipcz_size != 16 && avail > 8) {
		memcpy(&ipcz_size, base + off + 8, 2);
		off += 8;
		avail -= 8;
	}
	if (ipcz_size < 8 || avail < (mach_msg_size_t)ipcz_size + 44) {
		return 0;
	}
	proto = 0;
	memcpy(&proto, base + off + ipcz_size + 40, 4);
	return proto;
}

static uint64_t
machport_ipcz_sublink20(const mach_msg_header_t *msg)
{
	const unsigned char *base;
	mach_msg_size_t off, msg_size, avail;
	uint64_t sublink;
	uint16_t ipcz_size;

	if (machport_ipcz_msgid(msg) != 20) {
		return ~(uint64_t)0;
	}
	base = (const unsigned char *)msg;
	msg_size = msg->msgh_size;
	off = machport_chrome_ipcz_off(msg, NULL);
	if (off + 24 > msg_size) {
		return ~(uint64_t)0;
	}
	avail = msg_size - off;
	ipcz_size = 0;
	memcpy(&ipcz_size, base + off, 2);
	if (ipcz_size != 16 && avail > 8) {
		memcpy(&ipcz_size, base + off + 8, 2);
		off += 8;
		avail -= 8;
	}
	if (ipcz_size < 8 || avail < (mach_msg_size_t)ipcz_size + 40) {
		return ~(uint64_t)0;
	}
	sublink = ~(uint64_t)0;
	memcpy(&sublink, base + off + ipcz_size + 32, 8);
	return sublink;
}

static int
machport_ipcz_bypass34(const mach_msg_header_t *msg, uint64_t *sl,
    uint64_t *new_sl)
{
	const unsigned char *base;
	mach_msg_size_t off, msg_size, avail;
	uint16_t ipcz_size;
	uint64_t old_sl, nsl;

	if (sl != NULL) {
		*sl = ~(uint64_t)0;
	}
	if (new_sl != NULL) {
		*new_sl = ~(uint64_t)0;
	}
	if (machport_ipcz_msgid(msg) != 34) {
		return 0;
	}
	base = (const unsigned char *)msg;
	msg_size = msg->msgh_size;
	off = machport_chrome_ipcz_off(msg, NULL);
	if (off + 24 > msg_size) {
		return 0;
	}
	avail = msg_size - off;
	ipcz_size = 0;
	memcpy(&ipcz_size, base + off, 2);
	if (ipcz_size != 16 && avail > 8) {
		memcpy(&ipcz_size, base + off + 8, 2);
		off += 8;
		avail -= 8;
	}
	if (ipcz_size < 8 || avail < (mach_msg_size_t)ipcz_size + 48) {
		return 0;
	}
	old_sl = ~(uint64_t)0;
	nsl = ~(uint64_t)0;
	memcpy(&old_sl, base + off + ipcz_size + 32, 8);
	memcpy(&nsl, base + off + ipcz_size + 40, 8);
	if (sl != NULL) {
		*sl = old_sl;
	}
	if (new_sl != NULL) {
		*new_sl = nsl;
	}
	return 1;
}

static void
machport_helper_34_note(const char *dir, const mach_msg_header_t *msg,
    mach_port_t port)
{
	uint64_t sl, nsl;
	int i;

	if (dir == NULL || msg == NULL) {
		return;
	}
	if (!machport_ipcz_bypass34(msg, &sl, &nsl)) {
		return;
	}
	(void)machport_helper_34_vs_ping_v1;
	if (machport_h34_n < 8) {
		i = machport_h34_n;
		machport_h34_sl[i] = sl;
		machport_h34_new[i] = nsl;
		machport_h34_send[i] = (dir[0] == 's');
		machport_h34_n++;
	}
	fprintf(stderr,
	    "helper_34_vs_ping_v1 dir=%s pid=%d port=%u sl=%llu new_sl=%llu ping_seen=%d n34=%d nest=%d stash34=%d nmsg=%d\n",
	    dir, (int)getpid(), (unsigned)port, (unsigned long long)sl,
	    (unsigned long long)nsl, machport_hping_seen, machport_h34_n,
	    machport_in_dispatch, machport_stash34_has(port),
	    machport_msgcount(port));
	fflush(stderr);
	machport_knote_log(
	    "helper_34_vs_ping_v1 dir=%s port=%u sl=%llu new_sl=%llu ping_seen=%d",
	    dir, (unsigned)port, (unsigned long long)sl,
	    (unsigned long long)nsl, machport_hping_seen);
}

/*
 * Parse AcceptParcel router descriptors from the helper's received
 * Mach buffer (same offsets as dserver accept20_router_v1). sl=17
 * seq=2 is SIMPLE (ndesc=0); routers are in the ipcz payload, not
 * Mach COMPLEX. Do not rewrite sl. Do not dest-bind.
 */
static void
machport_accept20_chrome_note(const mach_msg_header_t *msg, mach_port_t port,
    uint64_t sl, unsigned seq)
{
	const unsigned char *base;
	const unsigned char *node;
	mach_msg_size_t off, msg_size, avail;
	uint16_t ipcz_size;
	uint32_t num_bytes;
	uint32_t h_off, r_off, hn, rn, rbytes;
	uint64_t nsl, dec, next_in, dec_len, frag_buf;
	uint32_t frag_off, frag_sz;
	unsigned char flags;
	unsigned ndesc;
	unsigned complex;

	if (msg == NULL) {
		return;
	}
	base = (const unsigned char *)msg;
	msg_size = msg->msgh_size;
	complex = (msg->msgh_bits & MACH_MSGH_BITS_COMPLEX) != 0;
	ndesc = 0;
	off = machport_chrome_ipcz_off(msg, &ndesc);
	if (off + 24 > msg_size) {
		return;
	}
	avail = msg_size - off;
	ipcz_size = 0;
	num_bytes = 0;
	memcpy(&ipcz_size, base + off, 2);
	memcpy(&num_bytes, base + off + 4, 4);
	if (ipcz_size != 16 && avail > 8) {
		memcpy(&ipcz_size, base + off + 8, 2);
		memcpy(&num_bytes, base + off + 12, 4);
		off += 8;
		avail -= 8;
	}
	if (ipcz_size < 8 || avail < (mach_msg_size_t)ipcz_size + 88) {
		if (sl == 17 || (sl > 1 && sl < 64 && seq <= 4)) {
			fprintf(stderr,
			    "helper_nodelink_routers_v1 desc_short pid=%d port=%u sl=%llu seq=%u off=%u ipcz=%u avail=%u complex=%u ndesc=%u\n",
			    (int)getpid(), (unsigned)port,
			    (unsigned long long)sl, seq, (unsigned)off,
			    (unsigned)ipcz_size, (unsigned)avail, complex,
			    ndesc);
			fflush(stderr);
		}
		return;
	}
	node = base + off + ipcz_size;
	avail -= ipcz_size;
	h_off = 0;
	r_off = 0;
	hn = 0;
	rn = 0;
	rbytes = 0;
	nsl = ~(uint64_t)0;
	dec = ~(uint64_t)0;
	next_in = ~(uint64_t)0;
	dec_len = ~(uint64_t)0;
	frag_buf = ~(uint64_t)0;
	frag_off = 0;
	frag_sz = 0;
	flags = 0;
	if (avail >= 84) {
		memcpy(&h_off, node + 76, 4);
		memcpy(&r_off, node + 80, 4);
	}
	if (h_off != 0 && h_off + 8 <= avail) {
		memcpy(&hn, node + h_off + 4, 4);
	}
	if (r_off != 0 && r_off + 8 <= avail) {
		memcpy(&rbytes, node + r_off, 4);
		memcpy(&rn, node + r_off + 4, 4);
	}
	if (rn > 0 && r_off + 8 + 72 <= avail) {
		memcpy(&nsl, node + r_off + 8 + 8, 8);
		memcpy(&frag_buf, node + r_off + 8 + 16, 8);
		memcpy(&frag_off, node + r_off + 8 + 24, 4);
		memcpy(&frag_sz, node + r_off + 8 + 28, 4);
		memcpy(&dec, node + r_off + 8 + 32, 8);
		memcpy(&next_in, node + r_off + 8 + 48, 8);
		memcpy(&dec_len, node + r_off + 8 + 56, 8);
		memcpy(&flags, node + r_off + 8 + 64, 1);
	}
	if (rn > 0 || sl == 17 || sl == 23) {
		fprintf(stderr,
		    "helper_nodelink_routers_v1 desc pid=%d port=%u sl=%llu seq=%u complex=%u ndesc=%u nhandles=%u nrouters=%u r_off=%u new_sl=%llu decaying=%llu next_in=%llu dec_len=%llu flags=0x%x frag_buf=%llu frag_off=0x%x frag_sz=0x%x msz=%u ipcz_off=%u\n",
		    (int)getpid(), (unsigned)port, (unsigned long long)sl,
		    seq, complex, ndesc, hn, rn, r_off,
		    (unsigned long long)nsl, (unsigned long long)dec,
		    (unsigned long long)next_in, (unsigned long long)dec_len,
		    (unsigned)flags, (unsigned long long)frag_buf, frag_off,
		    frag_sz, (unsigned)msg_size, (unsigned)off);
		fflush(stderr);
		machport_knote_log(
		    "helper_nodelink_routers_v1 desc sl=%llu seq=%u nrouters=%u new_sl=%llu decaying=%llu flags=0x%x frag_off=0x%x frag_sz=0x%x complex=%u",
		    (unsigned long long)sl, seq, rn, (unsigned long long)nsl,
		    (unsigned long long)dec, (unsigned)flags, frag_off, frag_sz,
		    complex);
	}
	if (rn > 0) {
		uint32_t d_off = 0;
		uint32_t name0 = 0;
		uint32_t name1 = 0;
		uint32_t ch_type = 0xffffffffu;
		unsigned o;
		char nhex[193];

		machport_desc_sl = sl;
		machport_desc_seq = seq;
		machport_desc_complex = complex;
		machport_desc_ndesc = ndesc;
		machport_desc_nhandles = hn;
		machport_desc_nrouters = rn;
		machport_desc_roff = r_off;
		machport_desc_newsl = nsl;
		machport_desc_decaying = dec;
		machport_desc_next_in = next_in;
		machport_desc_dec_len = dec_len;
		machport_desc_flags = flags;
		machport_desc_msz = msg_size;
		machport_desc_frag_buf = frag_buf;
		machport_desc_frag_off = frag_off;
		machport_desc_frag_sz = frag_sz;
		if (avail >= 76) {
			memcpy(&d_off, node + 72, 4);
		}
		nhex[0] = 0;
		if (avail >= 96) {
			machport_hex_n(node, 96, nhex, sizeof(nhex));
		}
		for (o = 0; o + 24 <= avail && o < 400; o += 4) {
			uint32_t nb = 0, ver = 0, iid = 0, fl = 0;

			memcpy(&nb, node + o, 4);
			memcpy(&ver, node + o + 4, 4);
			if (ver == 3 && nb >= 24 && nb <= 512) {
				memcpy(&iid, node + o + 8, 4);
				memcpy(&fl, node + o + 16, 4);
				memcpy(&name0, node + o + 8, 4);
				memcpy(&name1, node + o + 12, 4);
				if (o >= 8) {
					uint16_t hdrb = 0, typ = 0;

					memcpy(&hdrb, node + o - 4, 2);
					memcpy(&typ, node + o - 2, 2);
					if (hdrb == 64 || hdrb == 8 ||
					    hdrb == 16) {
						ch_type = typ;
					}
				}
				(void)iid;
				(void)fl;
				break;
			}
			nb = 0;
			ver = 0;
			memcpy(&nb, node + o, 4);
			memcpy(&ver, node + o + 4, 2);
			if (nb >= 24 && nb <= 512 && ver >= 8 && ver <= 128) {
				uint16_t typ = 0;

				memcpy(&typ, node + o + 6, 2);
				ch_type = typ;
				memcpy(&name0, node + o + 8, 4);
				memcpy(&name1, node + o + 12, 4);
				break;
			}
		}
		machport_desc_name0 = name0;
		machport_desc_name1 = name1;
		machport_desc_ch_type = ch_type;
		machport_desc_d_off = d_off;
		if (machport_xfer_n < 12) {
			struct machport_xfer *x =
			    &machport_xfers[machport_xfer_n++];

			x->recv_sl = sl;
			x->seq = seq;
			x->new_sl = nsl;
			x->decaying = dec;
			x->next_in = next_in;
			x->dec_len = dec_len;
			x->flags = flags;
			x->name0 = name0;
			x->name1 = name1;
			x->ch_type = ch_type;
		}
		fprintf(stderr,
		    "helper_childproc_sl_v1 xfer pid=%d port=%u recv_sl=%llu seq=%u new_sl=%llu decaying=%llu next_in=%llu dec_len=%llu flags=0x%x nhandles=%u name0=0x%x name1=0x%x ch_type=%u d_off=%u node96=%s\n",
		    (int)getpid(), (unsigned)port, (unsigned long long)sl, seq,
		    (unsigned long long)nsl, (unsigned long long)dec,
		    (unsigned long long)next_in, (unsigned long long)dec_len,
		    (unsigned)flags, hn, name0, name1, ch_type, d_off, nhex);
		fflush(stderr);
		machport_knote_log(
		    "helper_childproc_sl_v1 xfer recv_sl=%llu seq=%u new=%llu dec=%llu flags=0x%x name0=0x%x ch_type=%u",
		    (unsigned long long)sl, seq, (unsigned long long)nsl,
		    (unsigned long long)dec, (unsigned)flags, name0, ch_type);
	}
	(void)rbytes;
	(void)num_bytes;
}

static int
machport_ptr_ok(const void *p)
{
	uintptr_t a = (uintptr_t)p;

	if (p == NULL || (a & 7) != 0) {
		return 0;
	}
	if (a < 0x10000ull || a > 0x00007fffffffffffull) {
		return 0;
	}
	if ((a & 0xff000000ull) == 0xaddbba00ull) {
		return 0;
	}
	return machport_maps_rw(p);
}

static int
machport_has_vptr(const void *p)
{
	void *v;

	if (!machport_ptr_ok(p) || !machport_page_mapped(p)) {
		return 0;
	}
	memcpy(&v, p, 8);
	if (v == NULL) {
		return 0;
	}
	return machport_maps_vtab(v) || machport_in_comet(v) ||
	    machport_maps_code(v);
}

static int
machport_scan_sublink_slots(const void *base, unsigned nbytes, uint64_t ping_sl,
    int *hit23, int *nsl, uint64_t *sls, int maxsl, void **router23)
{
	unsigned off;
	int n = 0;
	int i;

	if (base == NULL || nbytes < 24) {
		return 0;
	}
	for (off = 0; off + 24 <= nbytes; off += 8) {
		uint64_t k;
		void *a;
		void *b;
		int dup;

		if (!machport_page_mapped((char *)base + off + 23)) {
			break;
		}
		memcpy(&k, (char *)base + off, 8);
		if (k > 64ull) {
			continue;
		}
		memcpy(&a, (char *)base + off + 8, 8);
		memcpy(&b, (char *)base + off + 16, 8);
		if (!machport_has_vptr(a) || !machport_has_vptr(b)) {
			continue;
		}
		dup = 0;
		for (i = 0; i < n; i++) {
			if (sls[i] == k) {
				dup = 1;
				break;
			}
		}
		if (!dup && n < maxsl) {
			sls[n++] = k;
		}
		if (k == ping_sl) {
			*hit23 = 1;
			if (router23 != NULL) {
				*router23 = b;
			}
		}
		if (k <= (uint64_t)MACHPORT_SL_MAX) {
			machport_sl_link[k] = a;
			machport_sl_router[k] = b;
		}
	}
	*nsl = n;
	return n;
}

static void *
machport_load_heap(const void *base, unsigned off)
{
	void *q;

	if (base == NULL || !machport_page_mapped((char *)base + off + 7)) {
		return NULL;
	}
	memcpy(&q, (char *)base + off, 8);
	if (!machport_ptr_ok(q) || !machport_page_mapped(q)) {
		return NULL;
	}
	return q;
}

/*
 * ChannelMac.delegate_ is a Channel::Delegate* (mojo ipcz Transport).
 * Skip MI subobjects that live inside ChannelMac itself (ot=-88 watcher).
 */
static void *
machport_channel_delegate(void *chan)
{
	unsigned off;
	void *first = NULL;

	if (chan == NULL) {
		return NULL;
	}
	for (off = 0x10; off <= 0x48; off += 8) {
		void *q = machport_load_heap(chan, off);

		if (q == NULL || q == chan) {
			continue;
		}
		if ((char *)q >= (char *)chan &&
		    (char *)q < (char *)chan + 0x200) {
			continue;
		}
		if (!machport_has_vptr(q)) {
			continue;
		}
		if (off == 0x30) {
			return q;
		}
		if (first == NULL) {
			first = q;
		}
	}
	return first;
}

static int
machport_merge_sls(uint64_t *dst, int nd, const uint64_t *src, int ns, int maxn)
{
	int i, j;

	for (i = 0; i < ns && nd < maxn; i++) {
		int dup = 0;

		for (j = 0; j < nd; j++) {
			if (dst[j] == src[i]) {
				dup = 1;
				break;
			}
		}
		if (!dup) {
			dst[nd++] = src[i];
		}
	}
	return nd;
}

/*
 * Walk one object as an absl slot array (16/24/32-byte {SublinkId, Ref, Ref})
 * and also follow a handful of child pointers that look like slot storage.
 */
static int
machport_scan_obj_sublinks(void *obj, uint64_t ping_sl, int *hit23,
    uint64_t *sls, int maxsl, void **router23, void **router22)
{
	uint64_t found[24];
	int nf = 0;
	int h = 0;
	void *r23 = NULL;
	unsigned off;
	int stride;

	if (obj == NULL) {
		return 0;
	}
	memset(found, 0, sizeof(found));
	(void)machport_scan_sublink_slots(obj, 0x1000u, ping_sl, &h, &nf,
	    found, 24, &r23);
	for (stride = 16; stride <= 32; stride += 8) {
		int h2 = 0;
		int n2 = 0;
		uint64_t f2[24];
		void *r2 = NULL;

		memset(f2, 0, sizeof(f2));
		for (off = 0; off + (unsigned)stride <= 0x1000u; off +=
		    (unsigned)stride) {
			uint64_t k;
			void *a;
			void *b;
			int i, dup;

			if (!machport_page_mapped((char *)obj + off + 23)) {
				break;
			}
			memcpy(&k, (char *)obj + off, 8);
			if (k > 64ull) {
				continue;
			}
			memcpy(&a, (char *)obj + off + 8, 8);
			b = NULL;
			if (stride >= 24) {
				memcpy(&b, (char *)obj + off + 16, 8);
			}
			if (!machport_has_vptr(a)) {
				continue;
			}
			if (stride >= 24 && !machport_has_vptr(b)) {
				continue;
			}
			dup = 0;
			for (i = 0; i < n2; i++) {
				if (f2[i] == k) {
					dup = 1;
					break;
				}
			}
			if (!dup && n2 < 24) {
				f2[n2++] = k;
			}
			if (k == ping_sl) {
				h2 = 1;
				r2 = (b != NULL) ? b : a;
			}
			if (k == 22 && router22 != NULL && *router22 == NULL) {
				*router22 = (b != NULL) ? b : a;
			}
			if (k <= (uint64_t)MACHPORT_SL_MAX) {
				if (a != NULL) {
					machport_sl_link[k] = a;
				}
				if (b != NULL || a != NULL) {
					machport_sl_router[k] =
					    (b != NULL) ? b : a;
				}
			}
		}
		if (n2 > nf) {
			nf = n2;
			h = h2;
			r23 = r2;
			memcpy(found, f2, sizeof(found));
		}
	}
	for (off = 0; off + 8 <= 0x1000u; off += 8) {
		void *slots = machport_load_heap(obj, off);
		int h2 = 0;
		int n2 = 0;
		uint64_t f2[24];
		void *r2 = NULL;

		if (slots == NULL || slots == obj) {
			continue;
		}
		memset(f2, 0, sizeof(f2));
		(void)machport_scan_sublink_slots(slots, 0x800u, ping_sl, &h2,
		    &n2, f2, 24, &r2);
		if (n2 > nf) {
			nf = n2;
			h = h2;
			r23 = r2;
			memcpy(found, f2, sizeof(found));
		} else if (h2 && !h) {
			h = 1;
			r23 = r2;
			nf = machport_merge_sls(found, nf, f2, n2, 24);
		} else if (n2 > 0) {
			nf = machport_merge_sls(found, nf, f2, n2, 24);
			if (h2) {
				h = 1;
				r23 = r2;
			}
		}
	}
	if (hit23 != NULL && h) {
		*hit23 = 1;
	}
	if (router23 != NULL && r23 != NULL) {
		*router23 = r23;
	}
	if (sls != NULL && nf > 0) {
		int ncopy = nf;

		if (ncopy > maxsl) {
			ncopy = maxsl;
		}
		memcpy(sls, found, (size_t)ncopy * sizeof(uint64_t));
		return ncopy;
	}
	return nf;
}

/*
 * ChannelMac Delegate* → DriverTransport → NodeLink::sublinks_.
 * Not a 48-object heap BFS (that capped out on ChannelMac internals).
 */
static void
machport_nodelink_sublinks_walk(void *chan, void *sub, uint64_t ping_sl,
    int *hit23, int *have17, int *have22, int *nsub, uint64_t *sls, int maxsl,
    void **router23, void **router22, void **del_out, void **dt_out,
    void **nl_out)
{
	void *del = NULL;
	void *dt_cands[8];
	void *nl_cands[8];
	int ndt = 0;
	int nnl = 0;
	int i;
	unsigned off;
	int best_n = 0;
	int best_h = 0;
	uint64_t best_sls[24];
	void *best_nl = NULL;
	void *best_dt = NULL;
	void *best_r23 = NULL;
	void *best_r22 = NULL;

	(void)machport_helper_nodelink_sublinks_v1;
	memset(dt_cands, 0, sizeof(dt_cands));
	memset(nl_cands, 0, sizeof(nl_cands));
	memset(best_sls, 0, sizeof(best_sls));
	memset(machport_sl_router, 0, sizeof(machport_sl_router));
	memset(machport_sl_link, 0, sizeof(machport_sl_link));
	if (hit23 != NULL) {
		*hit23 = 0;
	}
	if (have17 != NULL) {
		*have17 = 0;
	}
	if (have22 != NULL) {
		*have22 = 0;
	}
	if (nsub != NULL) {
		*nsub = 0;
	}
	if (del_out != NULL) {
		*del_out = NULL;
	}
	if (dt_out != NULL) {
		*dt_out = NULL;
	}
	if (nl_out != NULL) {
		*nl_out = NULL;
	}
	del = machport_channel_delegate(chan);
	if (del == NULL && sub != NULL && sub != chan) {
		del = machport_channel_delegate((char *)sub - 88);
		if (del == NULL) {
			del = machport_channel_delegate(sub);
		}
	}
	if (del_out != NULL) {
		*del_out = del;
	}
	if (del != NULL && machport_has_vptr(del)) {
		dt_cands[ndt++] = del;
	}
	if (del != NULL) {
		for (off = 0; off + 8 <= 0x280u && ndt < 8; off += 8) {
			void *q = machport_load_heap(del, off);
			int seen = 0;

			if (q == NULL || q == del || q == chan || q == sub) {
				continue;
			}
			if (!machport_has_vptr(q)) {
				continue;
			}
			for (i = 0; i < ndt; i++) {
				if (dt_cands[i] == q) {
					seen = 1;
					break;
				}
			}
			if (!seen) {
				dt_cands[ndt++] = q;
			}
		}
	}
	for (i = 0; i < ndt; i++) {
		unsigned o;

		for (o = 0; o + 8 <= 0x80u && nnl < 8; o += 8) {
			void *q = machport_load_heap(dt_cands[i], o);
			int seen = 0;
			int j;

			if (q == NULL || q == del || q == chan || q == sub) {
				continue;
			}
			if (!machport_has_vptr(q)) {
				continue;
			}
			for (j = 0; j < nnl; j++) {
				if (nl_cands[j] == q) {
					seen = 1;
					break;
				}
			}
			if (!seen) {
				nl_cands[nnl++] = q;
			}
		}
	}
	for (i = 0; i < nnl; i++) {
		uint64_t found[24];
		int nf = 0;
		int h = 0;
		void *r23 = NULL;
		void *r22 = NULL;
		int j, score;

		memset(found, 0, sizeof(found));
		nf = machport_scan_obj_sublinks(nl_cands[i], ping_sl, &h, found,
		    24, &r23, &r22);
		score = nf + (h ? 8 : 0);
		if (r22 != NULL) {
			score += 4;
		}
		if (score > best_n + (best_h ? 8 : 0) +
		    (best_r22 != NULL ? 4 : 0) ||
		    (h && !best_h)) {
			best_n = nf;
			best_h = h;
			best_nl = nl_cands[i];
			best_dt = (i < ndt) ? dt_cands[0] : NULL;
			best_r23 = r23;
			best_r22 = r22;
			memcpy(best_sls, found, sizeof(best_sls));
		}
	}
	if (best_nl == NULL) {
		for (i = 0; i < ndt; i++) {
			uint64_t found[24];
			int nf = 0;
			int h = 0;
			void *r23 = NULL;
			void *r22 = NULL;

			memset(found, 0, sizeof(found));
			nf = machport_scan_obj_sublinks(dt_cands[i], ping_sl, &h,
			    found, 24, &r23, &r22);
			if (nf > best_n) {
				best_n = nf;
				best_h = h;
				best_nl = dt_cands[i];
				best_dt = dt_cands[i];
				best_r23 = r23;
				best_r22 = r22;
				memcpy(best_sls, found, sizeof(best_sls));
			}
		}
	}
	if (dt_out != NULL) {
		*dt_out = best_dt != NULL ? best_dt :
		    (ndt > 0 ? dt_cands[0] : NULL);
	}
	if (nl_out != NULL) {
		*nl_out = best_nl;
	}
	if (hit23 != NULL) {
		*hit23 = best_h;
	}
	if (router23 != NULL) {
		*router23 = best_r23;
	}
	if (router22 != NULL) {
		*router22 = best_r22;
	}
	if (nsub != NULL) {
		*nsub = best_n;
	}
	if (sls != NULL) {
		int ncopy = best_n;

		if (ncopy > maxsl) {
			ncopy = maxsl;
		}
		if (ncopy > 0) {
			memcpy(sls, best_sls, (size_t)ncopy * sizeof(uint64_t));
		}
	}
	if (have17 != NULL || have22 != NULL) {
		int j;

		for (j = 0; j < best_n; j++) {
			if (best_sls[j] == 17 && have17 != NULL) {
				*have17 = 1;
			}
			if (best_sls[j] == 22 && have22 != NULL) {
				*have22 = 1;
			}
		if (best_sls[j] == 22 && router22 != NULL &&
		    *router22 == NULL) {
			/* already set if scan saw it */
		}
		}
	}
	if (best_nl != NULL) {
		int h = 0;
		int nf = 0;
		uint64_t found[24];
		void *r23 = NULL;
		void *r22 = NULL;

		memset(machport_sl_router, 0, sizeof(machport_sl_router));
		memset(machport_sl_link, 0, sizeof(machport_sl_link));
		memset(found, 0, sizeof(found));
		nf = machport_scan_obj_sublinks(best_nl, ping_sl, &h, found,
		    24, &r23, &r22);
		(void)nf;
		if (router23 != NULL && r23 != NULL) {
			*router23 = r23;
		}
		if (router22 != NULL && r22 != NULL) {
			*router22 = r22;
		}
	}
}

static uint64_t
machport_link_sublink(void *link)
{
	void *nl = NULL;
	uint64_t sl = ~(uint64_t)0;
	unsigned lo;

	if (link == NULL || !machport_has_vptr(link)) {
		return sl;
	}
	for (lo = 16; lo <= 40; lo += 8) {
		if (!machport_page_mapped((char *)link + lo + 15)) {
			break;
		}
		memcpy(&nl, (char *)link + lo, 8);
		memcpy(&sl, (char *)link + lo + 8, 8);
		if (machport_has_vptr(nl) && sl <= (uint64_t)MACHPORT_SL_MAX) {
			return sl;
		}
	}
	return ~(uint64_t)0;
}

static int
machport_router_vec_n(void *r, unsigned off)
{
	void *b = NULL;
	void *e = NULL;
	uintptr_t db, de;

	if (r == NULL || !machport_page_mapped((char *)r + off + 15)) {
		return -1;
	}
	memcpy(&b, (char *)r + off, 8);
	memcpy(&e, (char *)r + off + 8, 8);
	if (b == NULL || e == NULL || e < b) {
		return 0;
	}
	db = (uintptr_t)b;
	de = (uintptr_t)e;
	if ((de - db) > 4096ull || ((de - db) & 7ull) != 0) {
		return -1;
	}
	if (!machport_ptr_ok(b)) {
		return -1;
	}
	return (int)((de - db) / 8ull);
}

static void
machport_router_listen_dump(const char *tag, uint64_t sl, void *r, void *r23)
{
	char hex[513];
	char lsls[80];
	unsigned off;
	int nheap = 0;
	int nlink = 0;
	int ntrap = -1;
	int nedge = 0;
	int same23;
	uint64_t link_sls[8];
	int nls = 0;
	uint32_t type = 0;
	uint32_t refc = 0;
	uint64_t primary_sl = ~(uint64_t)0;
	int i;
	int ln;

	(void)machport_helper_childproc_sl_v1;
	hex[0] = 0;
	lsls[0] = '-';
	lsls[1] = 0;
	same23 = (r != NULL && r == r23);
	if (r == NULL || !machport_ptr_ok(r) ||
	    !machport_page_mapped((char *)r + 15)) {
		fprintf(stderr,
		    "helper_childproc_sl_v1 %s pid=%d sl=%llu getrouter=0 router=%p same23=0 listen=-1 proxy=-1\n",
		    tag, (int)getpid(), (unsigned long long)sl, r);
		fflush(stderr);
		return;
	}
	memcpy(&type, (char *)r + 8, 4);
	memcpy(&refc, (char *)r + 12, 4);
	if (machport_page_mapped((char *)r + 255)) {
		machport_hex_n((const unsigned char *)r, 256, hex,
		    sizeof(hex));
	}
	for (off = 0x18; off + 8 <= 0x1c0; off += 8) {
		void *q = machport_load_heap(r, off);
		uint64_t lsl;
		int dup;

		if (q == NULL) {
			continue;
		}
		nheap++;
		if (!machport_has_vptr(q)) {
			void *a = machport_load_heap(q, 0);
			void *b = machport_load_heap(q, 8);

			if (a != NULL && machport_has_vptr(a)) {
				nedge++;
				lsl = machport_link_sublink(a);
				if (lsl <= (uint64_t)MACHPORT_SL_MAX &&
				    nls < 8) {
					link_sls[nls++] = lsl;
					nlink++;
				}
				if (b != NULL && machport_has_vptr(b)) {
					lsl = machport_link_sublink(b);
					if (lsl <= (uint64_t)MACHPORT_SL_MAX &&
					    nls < 8) {
						link_sls[nls++] = lsl;
						nlink++;
					}
				}
			}
			continue;
		}
		lsl = machport_link_sublink(q);
		if (lsl > (uint64_t)MACHPORT_SL_MAX) {
			continue;
		}
		if (primary_sl == ~(uint64_t)0) {
			primary_sl = lsl;
		}
		dup = 0;
		for (i = 0; i < nls; i++) {
			if (link_sls[i] == lsl) {
				dup = 1;
				break;
			}
		}
		if (!dup && nls < 8) {
			link_sls[nls++] = lsl;
			nlink++;
		}
	}
	/*
	 * ipcz TrapSet is std::vector<Trap> after absl::Mutex
	 * (~0x80+). Trap is conditions+handler+context (24-40).
	 * Pointer-stride at 0x18-0xc0 is mutex/edge noise, not
	 * BindReceiver. ChildProcess listen is TrapSet n>=1.
	 */
	{
		for (off = 0x80; off + 16 <= 0x280; off += 8) {
			void *b = NULL;
			void *e = NULL;
			uintptr_t db, de, d;
			int vn;

			if (!machport_page_mapped((char *)r + off + 15)) {
				break;
			}
			memcpy(&b, (char *)r + off, 8);
			memcpy(&e, (char *)r + off + 8, 8);
			if (b == NULL || e == NULL || e < b) {
				continue;
			}
			db = (uintptr_t)b;
			de = (uintptr_t)e;
			d = de - db;
			if (d == 0 || d > 4096ull || !machport_ptr_ok(b)) {
				continue;
			}
			if ((d % 32ull) == 0) {
				vn = (int)(d / 32ull);
			} else if ((d % 24ull) == 0) {
				vn = (int)(d / 24ull);
			} else if ((d % 40ull) == 0) {
				vn = (int)(d / 40ull);
			} else {
				continue;
			}
			if (vn >= 1 && vn <= 8) {
				ntrap = vn;
				break;
			}
		}
	}
	ln = 0;
	for (i = 0; i < nls; i++) {
		if (ln < (int)sizeof(lsls) - 8) {
			ln += sprintf(lsls + ln, "%s%llu", i ? "," : "",
			    (unsigned long long)link_sls[i]);
		}
	}
	if (nls == 0) {
		lsls[0] = '-';
		lsls[1] = 0;
	}
	fprintf(stderr,
	    "helper_childproc_sl_v1 %s pid=%d sl=%llu getrouter=1 router=%p type=%u ref=%u same23=%d nheap=%d nlink=%d ntrap=%d nedge=%d primary_sl=%llu link_sls=%s listen=%d proxy=%d hex=%s\n",
	    tag, (int)getpid(), (unsigned long long)sl, r, type, refc, same23,
	    nheap, nlink, ntrap, nedge, (unsigned long long)primary_sl, lsls,
	    ntrap > 0, nedge > 0, hex);
	fflush(stderr);
	machport_knote_log(
	    "helper_childproc_sl_v1 %s sl=%llu getrouter=1 router=%p same23=%d ntrap=%d nedge=%d primary_sl=%llu listen=%d proxy=%d",
	    tag, (unsigned long long)sl, r, same23, ntrap, nedge,
	    (unsigned long long)primary_sl, ntrap > 0, nedge > 0);
	if (sl == 1 && ntrap > 0) {
		machport_childproc_bound = 1;
	}
}

static void
machport_helper_getrouter23_note(mach_port_t port, uint64_t ping_sl)
{
	char buf[4096];
	char line[1024];
	int fd;
	long n;
	int li = 0;
	int i;
	int getr = 0;
	int p1_eq = 0;
	uint64_t p1_sl = ~(uint64_t)0;
	int n128 = 0;
	int nmsg;
	int stash;
	unsigned long rls_va = 0;
	uint32_t rls_ref = 0;
	uint32_t rls_st = 0;
	char rls64hex[129];

	rls64hex[0] = 0;

	(void)machport_helper_getrouter23_v1;
	nmsg = machport_msgcount(port);
	stash = machport_stash34_has(port);
	for (i = 0; i < machport_h34_n; i++) {
		if (machport_h34_new[i] == ping_sl) {
			getr = 1;
		}
		if (machport_h34_sl[i] == 1 && machport_h34_send[i]) {
			p1_sl = machport_h34_new[i];
			p1_eq = (p1_sl == ping_sl);
		}
	}
	fd = (int)__linux_syscall(LINUX_SYS_openat, (long)LINUX_AT_FDCWD,
	    (long)(uintptr_t)"/proc/self/maps", (long)LINUX_O_RDONLY,
	    0L, 0L, 0L);
	if (fd >= 0) {
		for (;;) {
			n = __linux_syscall(LINUX_SYS_read, (long)fd,
			    (long)(uintptr_t)buf, (long)sizeof(buf), 0L, 0L, 0L);
			if (n <= 0) {
				break;
			}
			for (i = 0; i < (int)n; i++) {
				unsigned long start = 0, end = 0;
				char perms[8];
				unsigned char hdr[16];
				unsigned char p1[8];
				uint64_t next_buf, next_sl;
				uint32_t p1_st;

				if (buf[i] != '\n' && li < (int)sizeof(line) - 1) {
					line[li++] = buf[i];
					continue;
				}
				line[li] = '\0';
				li = 0;
				memset(perms, 0, sizeof(perms));
				if (sscanf(line, "%lx-%lx %7s", &start, &end,
				    perms) != 3 || end <= start) {
					continue;
				}
				if (perms[0] != 'r' || perms[1] != 'w') {
					continue;
				}
				if (end - start != 131072ul) {
					continue;
				}
				if (!machport_page_mapped((const void *)start) ||
				    !machport_page_mapped(
					(const void *)(start + 0x140))) {
					continue;
				}
				memcpy(hdr, (const void *)start, 16);
				memcpy(p1, (const void *)(start + 0x140), 8);
				memcpy(&next_buf, hdr, 8);
				memcpy(&next_sl, hdr + 8, 8);
				if (next_sl < 12 || next_sl > 100000ull ||
				    next_buf == 0 || next_buf > 4096ull) {
					continue;
				}
				memcpy(&p1_st, p1 + 4, 4);
				n128++;
				fprintf(stderr,
				    "helper_getrouter23_v1 mem pid=%d va=0x%lx next_sl=%llu next_buf=%llu p1_st=0x%x sl23_alloc=%d ping_sl=%llu\n",
				    (int)getpid(), start,
				    (unsigned long long)next_sl,
				    (unsigned long long)next_buf, p1_st,
				    next_sl > ping_sl, (unsigned long long)ping_sl);
				fflush(stderr);
				if (machport_page_mapped(
					(const void *)(start + 0x580 + 15))) {
					unsigned char rls[16];
					uint32_t st0, st1;

					memcpy(rls, (const void *)(start + 0x580),
					    16);
					memcpy(&st0, rls, 4);
					memcpy(&st1, rls + 4, 4);
					fprintf(stderr,
					    "helper_nodelink_routers_v1 rls580 pid=%d va=0x%lx st0=0x%x st1=0x%x hex=%02x%02x%02x%02x%02x%02x%02x%02x\n",
					    (int)getpid(), start, st0, st1,
					    rls[0], rls[1], rls[2], rls[3],
					    rls[4], rls[5], rls[6], rls[7]);
					fflush(stderr);
					if (rls_va == 0 || (st0 >= 1 && st1 != 0)) {
						rls_va = start;
						rls_ref = st0;
						rls_st = st1;
					}
				}
			}
		}
		(void)__linux_syscall(LINUX_SYS_close, (long)fd, 0L, 0L, 0L,
		    0L, 0L);
	}
	fprintf(stderr,
	    "helper_getrouter23_v1 Handle pid=%d port=%u ping_sl=%llu getrouter=%d p1_cur_sl=%llu p1_eq_ping=%d n34=%d n128=%d nmsg=%d stash34=%d ping_seen=1\n",
	    (int)getpid(), (unsigned)port, (unsigned long long)ping_sl, getr,
	    (unsigned long long)p1_sl, p1_eq, machport_h34_n, n128, nmsg,
	    stash);
	fflush(stderr);
	machport_knote_log(
	    "helper_getrouter23_v1 ping_sl=%llu getrouter=%d p1_cur_sl=%llu n34=%d nmsg=%d stash34=%d",
	    (unsigned long long)ping_sl, getr, (unsigned long long)p1_sl,
	    machport_h34_n, nmsg, stash);

	/*
	 * Real GetRouter: ChannelMac Delegate* → DriverTransport →
	 * NodeLink::sublinks_. Not a 48-object BFS (that never left
	 * ChannelMac). 34-table getrouter is the wrong signal.
	 */
	{
		void *chan = NULL;
		void *sub = NULL;
		void *del = NULL;
		void *dt = NULL;
		void *nl = NULL;
		int hit23 = 0;
		int have17 = 0;
		int have22 = 0;
		int best_n = 0;
		uint64_t best_sls[24];
		void *router23 = NULL;
		void *router22 = NULL;
		int wi;
		char slhex[160];
		int slhex_n = 0;
		int adopt_ok = 0;
		int adopt_why = 0;
		int mapped580 = 0;
		uint32_t blk0 = 0;

		(void)machport_helper_nodelink_routers_v1;
		machport_maps_refresh();
		for (wi = 0; wi < machport_watch_n; wi++) {
			if (machport_watches[wi].port == port) {
				chan = machport_watches[wi].chan;
				sub = machport_watches[wi].sub;
				break;
			}
		}
		memset(best_sls, 0, sizeof(best_sls));
		machport_nodelink_sublinks_walk(chan, sub, ping_sl, &hit23,
		    &have17, &have22, &best_n, best_sls, 24, &router23,
		    &router22, &del, &dt, &nl);
		for (i = 0; i < best_n; i++) {
			if (best_sls[i] == 17) {
				have17 = 1;
			}
			if (best_sls[i] == 22) {
				have22 = 1;
			}
			if (slhex_n < (int)sizeof(slhex) - 8) {
				slhex_n += sprintf(slhex + slhex_n, "%llu%s",
				    (unsigned long long)best_sls[i],
				    i + 1 < best_n ? "," : "");
			}
		}
		if (slhex_n == 0) {
			slhex[0] = '-';
			slhex[1] = 0;
		}
		rls64hex[0] = 0;
		if (rls_va != 0 &&
		    machport_page_mapped((const void *)(rls_va + 0x580 + 63))) {
			unsigned char rls64[64];
			unsigned char b0[4];

			mapped580 = 1;
			memcpy(rls64, (const void *)(rls_va + 0x580), 64);
			memcpy(&rls_ref, rls64, 4);
			memcpy(&rls_st, rls64 + 4, 4);
			machport_hex_n(rls64, 64, rls64hex, sizeof(rls64hex));
			if (machport_page_mapped(
				(const void *)(rls_va + 0x400 + 3))) {
				memcpy(b0, (const void *)(rls_va + 0x400), 4);
				memcpy(&blk0, b0, 4);
			}
		}
		/*
		 * AdoptFragmentRefIfValid: null / size<64 / offset%8 /
		 * GetFragment unmapped → nullptr before AddRemoteRouterLink.
		 * why: 1=null_buf 2=sz 3=align 4=unmap 0=ok
		 */
		if (machport_desc_frag_buf == ~(uint64_t)0) {
			adopt_why = 1;
		} else if (machport_desc_frag_sz < 64) {
			adopt_why = 2;
		} else if ((machport_desc_frag_off % 8u) != 0) {
			adopt_why = 3;
		} else if (!mapped580) {
			adopt_why = 4;
		} else {
			adopt_ok = 1;
		}
		fprintf(stderr,
		    "helper_nodelink_sublinks_v1 Ping Handle pid=%d port=%u ping_sl=%llu getrouter23=%d getrouter22=%d nsub=%d have17=%d have22=%d chan=%p del=%p dt=%p nl=%p router23=%p router22=%p sls=%s desc_new=%llu desc_decaying=%llu desc_flags=0x%x frag_buf=%llu frag_off=0x%x frag_sz=0x%x adopt_ok=%d adopt_why=%d rls_va=0x%lx ref=%u st=%u blk0=0x%x mapped580=%d\n",
		    (int)getpid(), (unsigned)port, (unsigned long long)ping_sl,
		    hit23, have22, best_n, have17, have22, chan, del, dt, nl,
		    router23, router22, slhex,
		    (unsigned long long)machport_desc_newsl,
		    (unsigned long long)machport_desc_decaying,
		    machport_desc_flags,
		    (unsigned long long)machport_desc_frag_buf,
		    machport_desc_frag_off, machport_desc_frag_sz, adopt_ok,
		    adopt_why, rls_va, rls_ref, rls_st, blk0, mapped580);
		fflush(stderr);
		machport_knote_log(
		    "helper_nodelink_sublinks_v1 getrouter23=%d getrouter22=%d nsub=%d del=%p nl=%p adopt_ok=%d why=%d ref=%u st=%u",
		    hit23, have22, best_n, del, nl, adopt_ok, adopt_why,
		    rls_ref, rls_st);
		fprintf(stderr,
		    "helper_nodelink_routers_v1 Ping Handle pid=%d port=%u ping_sl=%llu getrouter_real=%d nsub=%d have17=%d have22=%d nobj=%d chan=%p router23=%p sls=%s desc_sl=%llu desc_seq=%u desc_nrouters=%u desc_new=%llu desc_decaying=%llu desc_flags=0x%x desc_next_in=%llu desc_dec_len=%llu desc_complex=%u desc_ndesc=%u table34=%d\n",
		    (int)getpid(), (unsigned)port, (unsigned long long)ping_sl,
		    hit23, best_n, have17, have22, (nl != NULL), chan, router23,
		    slhex,
		    (unsigned long long)machport_desc_sl, machport_desc_seq,
		    machport_desc_nrouters,
		    (unsigned long long)machport_desc_newsl,
		    (unsigned long long)machport_desc_decaying,
		    machport_desc_flags,
		    (unsigned long long)machport_desc_next_in,
		    (unsigned long long)machport_desc_dec_len,
		    machport_desc_complex, machport_desc_ndesc, getr);
		fflush(stderr);
		machport_knote_log(
		    "helper_nodelink_routers_v1 ping_sl=%llu getrouter_real=%d nsub=%d have17=%d have22=%d desc_nrouters=%u desc_new=%llu desc_decaying=%llu flags=0x%x",
		    (unsigned long long)ping_sl, hit23, best_n, have17, have22,
		    machport_desc_nrouters,
		    (unsigned long long)machport_desc_newsl,
		    (unsigned long long)machport_desc_decaying,
		    machport_desc_flags);
		if (rls64hex[0] != 0) {
			fprintf(stderr,
			    "helper_nodelink_sublinks_v1 rls580_64 pid=%d va=0x%lx hex=%s\n",
			    (int)getpid(), rls_va, rls64hex);
			fflush(stderr);
		}
		if (router23 != NULL && machport_ptr_ok(router23) &&
		    machport_page_mapped((char *)router23 + 63)) {
			char rhex[129];

			machport_hex_n((const unsigned char *)router23, 64,
			    rhex, sizeof(rhex));
			fprintf(stderr,
			    "helper_nodelink_routers_v1 router23 pid=%d p=%p hex=%s\n",
			    (int)getpid(), router23, rhex);
			fflush(stderr);
		}
		if (router22 != NULL && machport_ptr_ok(router22) &&
		    machport_page_mapped((char *)router22 + 63)) {
			char rhex[129];

			machport_hex_n((const unsigned char *)router22, 64,
			    rhex, sizeof(rhex));
			fprintf(stderr,
			    "helper_nodelink_sublinks_v1 router22 pid=%d p=%p hex=%s\n",
			    (int)getpid(), router22, rhex);
			fflush(stderr);
		}
		{
			void *r15 = machport_sl_router[15];
			void *r1 = machport_sl_router[1];
			void *r0 = machport_sl_router[0];
			void *r2 = machport_sl_router[2];
			void *r22b = machport_sl_router[22];
			void *r23b = machport_sl_router[23] != NULL ?
			    machport_sl_router[23] : router23;
			uint64_t p1_link_sl = machport_link_sublink(
			    machport_sl_link[1]);
			unsigned xi;
			int created23 = -1;
			uint64_t created_recv = ~(uint64_t)0;
			unsigned created_seq = 0;
			unsigned created_flags = 0;
			uint32_t created_n0 = 0;

			if (r22b != NULL && router22 == NULL) {
				router22 = r22b;
			}
			machport_router_listen_dump("portal0", 0, r0, r23b);
			machport_router_listen_dump("portal1", 1, r1, r23b);
			machport_router_listen_dump("portal2", 2, r2, r23b);
			machport_router_listen_dump("sl15", 15, r15, r23b);
			machport_router_listen_dump("sl22", 22, r22b, r23b);
			machport_router_listen_dump("sl23", 23, r23b, r23b);
			for (xi = 0; xi < machport_xfer_n; xi++) {
				if (machport_xfers[xi].decaying == ping_sl ||
				    machport_xfers[xi].new_sl == ping_sl) {
					created23 = (int)xi;
					created_recv = machport_xfers[xi].recv_sl;
					created_seq = machport_xfers[xi].seq;
					created_flags = machport_xfers[xi].flags;
					created_n0 = machport_xfers[xi].name0;
				}
			}
			fprintf(stderr,
			    "helper_childproc_sl_v1 Ping Handle pid=%d port=%u ping_sl=%llu getrouter15=%d getrouter1=%d getrouter22=%d getrouter23=%d r15=%p r1=%p r22=%p r23=%p r15eq23=%d r1eq23=%d r22eq23=%d p1_link_sl=%llu p1_eq_ping=%d xfer_n=%u created23=%d created_recv=%llu created_seq=%u created_flags=0x%x created_name0=0x%x desc_new=%llu desc_decaying=%llu desc_flags=0x%x desc_name0=0x%x desc_name1=0x%x desc_ch_type=%u\n",
			    (int)getpid(), (unsigned)port,
			    (unsigned long long)ping_sl, r15 != NULL, r1 != NULL,
			    r22b != NULL, r23b != NULL, r15, r1, r22b, r23b,
			    r15 != NULL && r15 == r23b,
			    r1 != NULL && r1 == r23b,
			    r22b != NULL && r22b == r23b,
			    (unsigned long long)p1_link_sl,
			    p1_link_sl == ping_sl, machport_xfer_n, created23,
			    (unsigned long long)created_recv, created_seq,
			    created_flags, created_n0,
			    (unsigned long long)machport_desc_newsl,
			    (unsigned long long)machport_desc_decaying,
			    machport_desc_flags, machport_desc_name0,
			    machport_desc_name1, machport_desc_ch_type);
			fflush(stderr);
			machport_knote_log(
			    "helper_childproc_sl_v1 ping_sl=%llu getrouter15=%d getrouter1=%d r15eq23=%d r22eq23=%d p1_link_sl=%llu created_recv=%llu created_seq=%u flags=0x%x",
			    (unsigned long long)ping_sl, r15 != NULL,
			    r1 != NULL, r15 != NULL && r15 == r23b,
			    r22b != NULL && r22b == r23b,
			    (unsigned long long)p1_link_sl,
			    (unsigned long long)created_recv, created_seq,
			    created_flags);
		}
	}
}

static void
machport_invite_trap_dump(const char *why, mach_port_t port)
{
	static int n;

	(void)machport_invite_accept_trap_v1;
	if (why == NULL) {
		why = "?";
	}
	if (n >= 64) {
		return;
	}
	n++;
	fprintf(stderr,
	    "invite_accept_trap_v1 why=%s pid=%d port=%u helper=%d stor=%d net=%d browser=%d bound=%d nest=%d\n",
	    why, (int)getpid(), (unsigned)port, machport_is_helper(),
	    machport_is_storage(), machport_is_network(),
	    !machport_is_helper() && !machport_is_gpu_process(),
	    machport_childproc_bound, machport_in_dispatch);
	fflush(stderr);
	machport_knote_log(
	    "invite_accept_trap_v1 why=%s port=%u helper=%d stor=%d bound=%d",
	    why, (unsigned)port, machport_is_helper(),
	    machport_is_storage(), machport_childproc_bound);
	if (port != MACH_PORT_NULL) {
		machport_helper_getrouter23_note(port, 1);
	}
}

static uint32_t
machport_trailer_audit_pid(const mach_msg_header_t *msg)
{
	const unsigned char *tr;
	uint32_t pid = 0;
	mach_msg_size_t msz;

	if (msg == NULL) {
		return 0;
	}
	msz = msg->msgh_size;
	tr = (const unsigned char *)msg + ((msz + 3u) & ~3u);
	if (!machport_page_mapped(tr + 40)) {
		return 0;
	}
	memcpy(&pid, tr + 40, 4);
	return pid;
}

static void
machport_channelmac_bound_note(mach_port_t port, unsigned sl, unsigned seq,
    uint32_t nb, uint32_t iid, uint32_t flags, uint32_t audit_pid)
{
	int i;
	void *chan = NULL;
	void *sub = NULL;
	void *peer = NULL;
	pthread_mutex_t *held;
	unsigned off;
	uint32_t recv_name = 0;
	uint32_t send_name = 0;
	uint32_t peer_pid = 0xffffffffu;
	int leak = -1;
	int hs_done = -1;
	int reject = -1;
	int watches = 0;
	char hex[769];

	(void)machport_channelmac_bound_v1;
	for (i = 0; i < machport_watch_n; i++) {
		if (machport_watches[i].port == port) {
			watches++;
			if (chan == NULL || machport_watches[i].ot_top == -88) {
				chan = machport_watches[i].chan;
				sub = machport_watches[i].sub;
			}
		}
	}
	held = machport_channelmac_held_lock(chan);
	hex[0] = 0;
	if (chan != NULL && machport_maps_rw(chan) &&
	    machport_page_mapped((char *)chan + 383)) {
		machport_hex_n((const unsigned char *)chan, 384, hex,
		    sizeof(hex));
		for (off = 0x40; off + 12 <= 0x130; off += 4) {
			uint32_t v;

			if (!machport_page_mapped((char *)chan + off + 11)) {
				continue;
			}
			memcpy(&v, (char *)chan + off, 4);
			if (v != (uint32_t)port) {
				continue;
			}
			recv_name = v;
			memcpy(&send_name, (char *)chan + off + 4, 4);
			leak = ((unsigned char *)chan)[off + 8];
			hs_done = ((unsigned char *)chan)[off + 9];
			memcpy(&peer, (char *)chan + off + 16, 8);
			break;
		}
		if (peer != NULL && machport_maps_rw(peer) &&
		    machport_page_mapped((char *)peer + 20)) {
			memcpy(&peer_pid, (char *)peer + 20, 4);
		}
		if (held != NULL) {
			unsigned char *after = (unsigned char *)held + 64;

			if (machport_page_mapped(after)) {
				reject = *after;
			}
		}
	}
	fprintf(stderr,
	    "channelmac_bound_v1 pid=%d port=%u sl=%u seq=%u nb=%u iid=%u flags=%u nest=%d this=sub=%p chan=%p watchers=%d lock_held=%d recv=0x%x send=0x%x leak=%d handshake_done=%d reject_writes=%d peer=%p peer_pid=%u trailer_pid=%u hex=%s\n",
	    (int)getpid(), (unsigned)port, sl, seq, nb, iid, flags,
	    machport_in_dispatch, sub, chan, watches, held != NULL,
	    recv_name, send_name, leak, hs_done, reject, peer, peer_pid,
	    audit_pid, hex);
	fflush(stderr);
	machport_knote_log(
	    "channelmac_bound_v1 port=%u sl=%u seq=%u flags=%u nest=%d recv=0x%x send=0x%x hs=%d reject=%d peer_pid=%u trailer_pid=%u",
	    (unsigned)port, sl, seq, flags, machport_in_dispatch,
	    recv_name, send_name, hs_done, reject, peer_pid, audit_pid);
}

static int
machport_mojo_hdr(const mach_msg_header_t *msg, uint32_t *iid,
    uint32_t *name, uint32_t *flags)
{
	const unsigned char *base;
	const unsigned char *imsg;
	mach_msg_size_t off, msg_size, avail, o;
	uint16_t ipcz_size;
	uint32_t dh_nb, dh_ver;

	if (iid != NULL) {
		*iid = 0xffffffffu;
	}
	if (name != NULL) {
		*name = 0xffffffffu;
	}
	if (flags != NULL) {
		*flags = 0xffffffffu;
	}
	if (msg == NULL || msg->msgh_id != (mach_msg_id_t)0x4d4f4a4fu) {
		return 0;
	}
	base = (const unsigned char *)msg;
	msg_size = msg->msgh_size;
	if (msg_size < 48 || msg_size > 8192) {
		return 0;
	}
	off = machport_chrome_ipcz_off(msg, NULL);
	if (off + 24 > msg_size) {
		return 0;
	}
	avail = msg_size - off;
	ipcz_size = 0;
	memcpy(&ipcz_size, base + off, 2);
	if (ipcz_size != 16 && avail > 8) {
		memcpy(&ipcz_size, base + off + 8, 2);
		off += 8;
		avail -= 8;
	}
	if (ipcz_size < 8 || avail < (mach_msg_size_t)ipcz_size + 24) {
		return 0;
	}
	imsg = base + off + ipcz_size;
	avail -= ipcz_size;
	if (avail >= 104) {
		memcpy(&dh_nb, imsg + 80, 4);
		memcpy(&dh_ver, imsg + 84, 4);
		if (dh_ver == 3 && dh_nb >= 24 && dh_nb <= 256) {
			if (iid != NULL) {
				memcpy(iid, imsg + 88, 4);
			}
			if (name != NULL) {
				memcpy(name, imsg + 92, 4);
			}
			if (flags != NULL) {
				memcpy(flags, imsg + 96, 4);
			}
			return 1;
		}
	}
	for (o = 24; o + 24 <= avail && o < 256; o += 8) {
		memcpy(&dh_nb, imsg + o, 4);
		memcpy(&dh_ver, imsg + o + 4, 4);
		if (dh_ver == 3 && dh_nb >= 24 && dh_nb <= 256) {
			if (iid != NULL) {
				memcpy(iid, imsg + o + 8, 4);
			}
			if (name != NULL) {
				memcpy(name, imsg + o + 12, 4);
			}
			if (flags != NULL) {
				memcpy(flags, imsg + o + 16, 4);
			}
			return 1;
		}
	}
	return 0;
}

static int
machport_mojo_from_msg(const mach_msg_header_t *msg, uint32_t *iid,
    uint32_t *flags)
{
	return machport_mojo_hdr(msg, iid, NULL, flags);
}

/*
 * Channel::Header {u32 num_bytes, u16 num_header_bytes, u16 type}.
 * type 2/3 is Channel PING/PONG (not Mojo expects-response).
 * hdrb=64 is Mojo DataHeader, not Channel.
 */
static uint16_t
machport_msg_ch_type(const mach_msg_header_t *msg, uint16_t *hdrb_out,
    uint32_t *nb_out)
{
	const unsigned char *base;
	const unsigned char *p;
	mach_msg_size_t off, msg_size, avail;
	uint32_t nb;
	uint16_t hdrb;
	uint16_t typ;
	uint16_t ipcz_size;

	if (hdrb_out != NULL) {
		*hdrb_out = 0xffff;
	}
	if (nb_out != NULL) {
		*nb_out = 0;
	}
	if (msg == NULL) {
		return 0xffff;
	}
	base = (const unsigned char *)msg;
	msg_size = msg->msgh_size;
	if (msg_size < 32 || msg_size > 8192) {
		return 0xffff;
	}
	off = machport_chrome_ipcz_off(msg, NULL);
	if (off == 0 || off + 8 > msg_size) {
		off = 24;
		if ((msg->msgh_bits & MACH_MSGH_BITS_COMPLEX) != 0) {
			off = 28;
		}
	}
	if (off + 8 > msg_size) {
		return 0xffff;
	}
	p = base + off;
	avail = msg_size - off;
	ipcz_size = 0;
	memcpy(&ipcz_size, p, 2);
	if (ipcz_size == 16 || ipcz_size == 24) {
		return 0xffff;
	}
	memcpy(&nb, p, 4);
	memcpy(&hdrb, p + 4, 2);
	memcpy(&typ, p + 6, 2);
	if (nb >= 8 && nb <= 4096 &&
	    (hdrb == 8 || hdrb == 16 || hdrb == 24 || hdrb == 32) &&
	    typ <= 6) {
		if (hdrb_out != NULL) {
			*hdrb_out = hdrb;
		}
		if (nb_out != NULL) {
			*nb_out = nb;
		}
		return typ;
	}
	if (avail > 8) {
		memcpy(&nb, p + 8, 4);
		memcpy(&hdrb, p + 12, 2);
		memcpy(&typ, p + 14, 2);
		if (nb >= 8 && nb <= 4096 &&
		    (hdrb == 8 || hdrb == 16 || hdrb == 24 || hdrb == 32) &&
		    typ <= 6) {
			if (hdrb_out != NULL) {
				*hdrb_out = hdrb;
			}
			if (nb_out != NULL) {
				*nb_out = nb;
			}
			return typ;
		}
	}
	return 0xffff;
}

static unsigned
machport_ipcz_num_bytes(const mach_msg_header_t *msg)
{
	const unsigned char *base;
	mach_msg_size_t off, msg_size, avail;
	uint32_t num_bytes;
	uint16_t ipcz_size;

	if (msg == NULL || msg->msgh_id != (mach_msg_id_t)0x4d4f4a4fu) {
		return 0;
	}
	base = (const unsigned char *)msg;
	msg_size = msg->msgh_size;
	if (msg_size < 48 || msg_size > 8192) {
		return 0;
	}
	off = machport_chrome_ipcz_off(msg, NULL);
	if (off + 8 > msg_size) {
		return 0;
	}
	avail = msg_size - off;
	ipcz_size = 0;
	num_bytes = 0;
	memcpy(&ipcz_size, base + off, 2);
	memcpy(&num_bytes, base + off + 4, 4);
	if (ipcz_size != 16 && avail > 8) {
		memcpy(&ipcz_size, base + off + 8, 2);
		memcpy(&num_bytes, base + off + 12, 4);
	}
	(void)ipcz_size;
	return num_bytes;
}

/*
 * Arch16: dump Mach + ipcz AcceptParcel vs public layout.
 * ChannelMac inline: header 24 + body 4 + uint64 size + Channel payload.
 * Channel ipcz header is 16 bytes (num_header_bytes=16). ipcz
 * MessageHeader is 32 bytes (msgid at [2]). AcceptParcel sublink at
 * +32, sequence at +40. proto=4 here is that sequence, not Mojo.
 * Do not invent Pong. Do not retarget leftover-20.
 */
static void
machport_hex_n(const unsigned char *p, unsigned n, char *out, unsigned cap)
{
	static const char hx[] = "0123456789abcdef";
	unsigned i;
	unsigned maxn;

	if (out == NULL || cap < 3) {
		return;
	}
	maxn = (cap - 1) / 2;
	if (n > maxn) {
		n = maxn;
	}
	for (i = 0; i < n; i++) {
		out[i * 2] = hx[p[i] >> 4];
		out[i * 2 + 1] = hx[p[i] & 0xf];
	}
	out[n * 2] = 0;
}

static void
machport_ping_gap_note(const mach_msg_header_t *msg, mach_port_t port)
{
	const unsigned char *base;
	const unsigned char *ipcz;
	const unsigned char *imsg;
	char hex[385];
	char thex[97];
	char idch[5];
	void *thisp = NULL;
	void *sub = NULL;
	void *chan = NULL;
	pthread_mutex_t *held;
	long ot = 0;
	unsigned proto;
	unsigned nbytes;
	unsigned i;
	unsigned n;
	unsigned complex;
	unsigned ndesc;
	unsigned desc12_type;
	unsigned desc16_type;
	uint32_t bits;
	uint32_t msz;
	uint32_t remote;
	uint32_t local;
	uint32_t voucher;
	int32_t mid;
	uint64_t inline_u64;
	uint16_t hdr16;
	uint16_t nh;
	uint32_t ch_nbytes;
	unsigned char msgid;
	uint64_t sublink;
	uint64_t seq64;
	uint32_t seq32;
	uint32_t mojo_iid;
	uint32_t mojo_name;
	uint32_t mojo_flags;
	uint32_t tr_type;
	uint32_t tr_size;
	uint32_t audit_pid;
	uint32_t audit0;
	uint32_t audit1;
	mach_msg_size_t hsz;
	mach_msg_size_t off;
	mach_msg_size_t avail = 0;
	const unsigned char *tr;
	static int gap_n;

	(void)machport_ping_gap_v1;
	(void)machport_parcel_dump_v1;
	if (msg == NULL || gap_n >= 32) {
		return;
	}
	proto = machport_ipcz_proto20(msg);
	nbytes = machport_ipcz_num_bytes(msg);
	for (i = 0; i < (unsigned)machport_watch_n; i++) {
		if (machport_watches[i].port == port) {
			thisp = machport_watches[i].thisp;
			sub = machport_watches[i].sub;
			chan = machport_watches[i].chan;
			ot = machport_watches[i].ot_top;
			break;
		}
	}
	base = (const unsigned char *)msg;
	n = 16;
	if ((unsigned)msg->msgh_size < n) {
		n = (unsigned)msg->msgh_size;
	}
	machport_hex_n(base, n, hex, sizeof(hex));
	gap_n++;
	fprintf(stderr,
	    "channelmac_ping_gap_v1 pid=%d port=%u proto=%u num_bytes=%u size=%u nest=%d this=%p sub=%p ot=%ld hex=%s\n",
	    (int)getpid(), (unsigned)port, proto, nbytes,
	    (unsigned)msg->msgh_size, machport_in_dispatch, thisp, sub, ot, hex);
	fflush(stderr);
	machport_knote_log(
	    "channelmac_ping_gap_v1 port=%u proto=%u num_bytes=%u nest=%d this=%p sub=%p",
	    (unsigned)port, proto, nbytes, machport_in_dispatch, thisp, sub);

	memcpy(&bits, base, 4);
	memcpy(&msz, base + 4, 4);
	memcpy(&remote, base + 8, 4);
	memcpy(&local, base + 12, 4);
	memcpy(&voucher, base + 16, 4);
	memcpy(&mid, base + 20, 4);
	complex = (bits & MACH_MSGH_BITS_COMPLEX) ? 1 : 0;
	ndesc = 0;
	hsz = (mach_msg_size_t)sizeof(mach_msg_header_t);
	if (msz >= hsz + sizeof(mach_msg_body_t)) {
		const mach_msg_body_t *body =
		    (const mach_msg_body_t *)(base + hsz);

		ndesc = body->msgh_descriptor_count;
	}
	desc12_type = 0xff;
	desc16_type = 0xff;
	if (complex && ndesc > 0 && msz >= hsz + 4 + 12) {
		desc12_type = base[hsz + 4 + 11];
	}
	if (complex && ndesc > 0 && msz >= hsz + 4 + 16) {
		desc16_type = base[hsz + 4 + 15];
	}
	idch[0] = (char)((mid >> 24) & 0xff);
	idch[1] = (char)((mid >> 16) & 0xff);
	idch[2] = (char)((mid >> 8) & 0xff);
	idch[3] = (char)(mid & 0xff);
	idch[4] = 0;
	held = machport_channelmac_held_lock(chan);
	fprintf(stderr,
	    "channelmac_parcel_dump_v1 mach pid=%d port=%u bits=0x%x size=%u id=0x%x id4=%s local=0x%x remote=0x%x voucher=0x%x complex=%u ndesc=%u nest=%d desc_port=%zu desc_ool=%zu desc12_type=%u desc16_type=%u lock_held=%d this=%p sub=%p chan=%p ot=%ld\n",
	    (int)getpid(), (unsigned)port, bits, msz, (unsigned)mid, idch,
	    local, remote, voucher, complex, ndesc, machport_in_dispatch,
	    sizeof(mach_msg_port_descriptor_t),
	    sizeof(mach_msg_ool_descriptor_t), desc12_type, desc16_type,
	    held != NULL, thisp, sub, chan, ot);
	fflush(stderr);

	off = hsz + (mach_msg_size_t)sizeof(mach_msg_body_t) +
	    ndesc * (mach_msg_size_t)sizeof(mach_msg_port_descriptor_t);
	if (complex && ndesc > 0) {
		/*
		 * Darling copyout_body shrinks kernel 16-byte
		 * descriptors to 12-byte userspace. Chrome LP64 OOL
		 * is 16. Try both payload origins.
		 */
		off = hsz + (mach_msg_size_t)sizeof(mach_msg_body_t) +
		    ndesc * 12u;
	}
	inline_u64 = 0;
	hdr16 = 0;
	nh = 0;
	ch_nbytes = 0;
	msgid = 0xff;
	sublink = ~(uint64_t)0;
	seq64 = ~(uint64_t)0;
	seq32 = 0xffffffffu;
	mojo_iid = 0xffffffffu;
	mojo_name = 0xffffffffu;
	mojo_flags = 0xffffffffu;
	ipcz = NULL;
	if (off + 8 <= msz) {
		memcpy(&inline_u64, base + off, 8);
		ipcz = base + off + 8;
		avail = msz - off - 8;
		if (avail >= 8) {
			memcpy(&hdr16, ipcz, 2);
			memcpy(&nh, ipcz + 2, 2);
			memcpy(&ch_nbytes, ipcz + 4, 4);
		}
		if (hdr16 != 16 && (inline_u64 & 0xffffull) == 16) {
			ipcz = base + off;
			avail = msz - off;
			memcpy(&hdr16, ipcz, 2);
			memcpy(&nh, ipcz + 2, 2);
			memcpy(&ch_nbytes, ipcz + 4, 4);
		}
		if (ipcz != NULL && hdr16 >= 8 &&
		    avail >= (mach_msg_size_t)hdr16 + 24) {
			imsg = ipcz + hdr16;
			msgid = imsg[2];
			if (avail >= (mach_msg_size_t)hdr16 + 48) {
				memcpy(&sublink, imsg + 32, 8);
				memcpy(&seq64, imsg + 40, 8);
				memcpy(&seq32, imsg + 40, 4);
			}
			/*
			 * parcel_data follows AcceptParcel params.
			 * Mojo header is 24+ bytes: iid, name, flags.
			 * Probe ipcz+hdr16+48 (after sublink+seq).
			 */
			if (avail >= (mach_msg_size_t)hdr16 + 24) {
				(void)machport_mojo_hdr(msg, &mojo_iid,
				    &mojo_name, &mojo_flags);
			}
		}
	}
	hex[0] = 0;
	if (ipcz != NULL && avail > 0) {
		n = avail;
		if (n > 184) {
			n = 184;
		}
		machport_hex_n(ipcz, n, hex, sizeof(hex));
	}
	fprintf(stderr,
	    "channelmac_parcel_dump_v1 ipcz pid=%d port=%u inline_u64=%llu hdr16=%u nhandles=%u ch_nbytes=%u ipcz_msgid=%u sl=%llu seq32=%u seq64=%llu mojo_iid=%u mojo_name=%u mojo_flags=%u off=%u payload=%s\n",
	    (int)getpid(), (unsigned)port,
	    (unsigned long long)inline_u64, hdr16, nh, ch_nbytes, msgid,
	    (unsigned long long)sublink, seq32, (unsigned long long)seq64,
	    mojo_iid, mojo_name, mojo_flags, (unsigned)off, hex);
	fflush(stderr);
	if (ipcz != NULL && avail >= 24 &&
	    ((sublink == 15 && (mojo_flags & 1u) != 0) ||
	    machport_msg_ch_type(msg, NULL, NULL) == 2 ||
	    machport_msg_ch_type(msg, NULL, NULL) == 3)) {
		unsigned o;
		uint32_t ch_nb = 0;
		uint16_t ch_hdrb = 0;
		uint16_t ch_type = 0xffff;
		uint32_t name0 = 0;
		uint32_t name1 = 0;

		ch_type = machport_msg_ch_type(msg, &ch_hdrb, &ch_nb);
		for (o = 0; o + 16 <= avail && o < 200 && ch_type == 0xffff; o += 4) {
			memcpy(&ch_nb, ipcz + o, 4);
			memcpy(&ch_hdrb, ipcz + o + 4, 2);
			memcpy(&ch_type, ipcz + o + 6, 2);
			if (ch_nb >= 8 && ch_nb <= 256 &&
			    (ch_hdrb == 8 || ch_hdrb == 16 || ch_hdrb == 24 ||
			    ch_hdrb == 32)) {
				if (o + 16 <= avail) {
					memcpy(&name0, ipcz + o + 8, 4);
					memcpy(&name1, ipcz + o + 12, 4);
				}
				break;
			}
			ch_type = 0xffff;
		}
		fprintf(stderr,
		    "helper_real_ping_v1 parcel pid=%d port=%u sl=%llu seq=%u nb=%u iid=%u flags=%u ch_nb=%u ch_hdrb=%u ch_type=%u name0=0x%x name1=0x%x\n",
		    (int)getpid(), (unsigned)port,
		    (unsigned long long)sublink, seq32, ch_nbytes, mojo_iid,
		    mojo_flags, ch_nb, ch_hdrb, ch_type, name0, name1);
		fflush(stderr);
		machport_knote_log(
		    "helper_real_ping_v1 parcel sl=%llu ch_type=%u flags=%u",
		    (unsigned long long)sublink, ch_type, mojo_flags);
	}

	tr_type = 0;
	tr_size = 0;
	audit_pid = 0;
	audit0 = 0;
	audit1 = 0;
	thex[0] = 0;
	tr = base + ((msz + 3u) & ~3u);
	if (machport_page_mapped(tr) && machport_page_mapped(tr + 48)) {
		memcpy(&tr_type, tr, 4);
		memcpy(&tr_size, tr + 4, 4);
		memcpy(&audit0, tr + 24, 4);
		memcpy(&audit1, tr + 28, 4);
		memcpy(&audit_pid, tr + 40, 4);
		machport_hex_n(tr, 48, thex, sizeof(thex));
	}
	fprintf(stderr,
	    "channelmac_parcel_dump_v1 trailer pid=%d port=%u tr_type=%u tr_size=%u audit0=%u audit1=%u audit_pid=%u hex=%s\n",
	    (int)getpid(), (unsigned)port, tr_type, tr_size, audit0, audit1,
	    audit_pid, thex);
	fflush(stderr);
	machport_knote_log(
	    "channelmac_parcel_dump_v1 port=%u id=0x%x ndesc=%u msgid=%u sl=%llu seq=%u nbytes=%u nest=%d lock_held=%d",
	    (unsigned)port, (unsigned)mid, ndesc, msgid,
	    (unsigned long long)sublink, seq32, ch_nbytes,
	    machport_in_dispatch, held != NULL);

	fprintf(stderr,
	    "samechan_bind_seq4_v1 pid=%d port=%u msgid=%u sl=%llu seq=%u nb=%u iid=%u flags=%u nest=%d lock_held=%d hdr16=%u parse_nb_off=4\n",
	    (int)getpid(), (unsigned)port, msgid,
	    (unsigned long long)sublink, seq32, ch_nbytes, mojo_iid,
	    mojo_flags, machport_in_dispatch, held != NULL, hdr16);
	fflush(stderr);

	if (!machport_in_dispatch && msgid == 20 &&
	    (nbytes == 184 || ch_nbytes == 184) &&
	    mojo_iid == 0 && (mojo_flags & 1u) != 0 &&
	    sublink == 15) {
		machport_ping_wait_send = 1;
		machport_ping_wait_sends = 0;
		machport_ping_wait_port = (unsigned)port;
		fprintf(stderr,
		    "channelmac_parcel_dump_v1 wait_send pid=%d port=%u nest=0 proto=4 nb=184 sl=15 local=0x%x dest_hint=childprocess\n",
		    (int)getpid(), (unsigned)port, local);
		fflush(stderr);
	}
}

static int
machport_stash34_has(mach_port_t port)
{
	int i;

	for (i = 0; i < MACHPORT_STASH34_MAX; i++) {
		if (machport_stash34[i].valid &&
		    machport_stash34[i].port == port) {
			return 1;
		}
	}
	return 0;
}

static int
machport_stash34_push(mach_port_t port, mach_msg_header_t *msg,
    mach_msg_option_t option, mach_msg_size_t rcv_size,
    mach_msg_return_t kr)
{
	struct machport_stash34_slot *s;
	mach_msg_size_t n;
	int i;

	(void)machport_dispatch_34_perport_v1;
	if (msg == NULL || rcv_size == 0 || machport_stash34_has(port)) {
		return 0;
	}
	s = NULL;
	for (i = 0; i < MACHPORT_STASH34_MAX; i++) {
		if (!machport_stash34[i].valid) {
			s = &machport_stash34[i];
			break;
		}
	}
	if (s == NULL) {
		return 0;
	}
	n = msg->msgh_size + REQUESTED_TRAILER_SIZE(option);
	if (n > rcv_size) {
		n = rcv_size;
	}
	if (n > sizeof(s->buf)) {
		n = sizeof(s->buf);
	}
	if (n == 0) {
		return 0;
	}
	memcpy(s->buf, msg, n);
	s->valid = 1;
	s->port = port;
	s->kr = kr;
	s->nbytes = n;
	return 1;
}

static int
machport_stash34_pop(mach_port_t port, mach_msg_header_t *msg,
    mach_msg_size_t rcv_size, mach_msg_return_t *kr)
{
	struct machport_stash34_slot *s;
	int i;

	if (msg == NULL) {
		return 0;
	}
	s = NULL;
	for (i = 0; i < MACHPORT_STASH34_MAX; i++) {
		if (machport_stash34[i].valid &&
		    machport_stash34[i].port == port) {
			s = &machport_stash34[i];
			break;
		}
	}
	if (s == NULL) {
		return 0;
	}
	if (s->nbytes > rcv_size) {
		return 0;
	}
	memcpy(msg, s->buf, s->nbytes);
	*kr = s->kr;
	s->valid = 0;
	s->port = MACH_PORT_NULL;
	s->nbytes = 0;
	return 1;
}

static int
machport_is_offer_copyout(const mach_msg_header_t *msg)
{
	unsigned id;

	if (msg == NULL || msg->msgh_id != (mach_msg_id_t)0x4d4f4a4fu) {
		return 0;
	}
	id = machport_ipcz_msgid(msg);
	if (id == 20 || id == 22 || id == 34 || id == 35) {
		return 0;
	}
	return 1;
}

/*
 * Leftover Offer copyout: receive_buffer_ is Offer. Do not fn()
 * inbound 20s (code233: FIFO-head 20, zero Ping proto=4, 34 not
 * posted). Leave Offer in place. Complete mjhs/Connect so the
 * browser can post 34. Nest=0 Handle of 34 is after Connect send
 * (machport_dispatch_34_after_connect). Do not nest-dispatch 34
 * during Accept/Ping send.
 */
static void
machport_dispatch_34_after_offer(mach_port_t port, mach_msg_header_t *offer,
    mach_msg_option_t option, mach_msg_size_t rcv_size)
{
	int i;
	int nmsg;
	mach_msg_size_t n;
	unsigned char save[8192];

	(void)machport_dispatch_34_after_offer_v1;
	(void)machport_dispatch_skip20_after_offer_v1;
	if (machport_after_offer_busy || machport_in_dispatch) {
		return;
	}
	if (machport_is_gpu_process() || offer == NULL) {
		return;
	}
	nmsg = machport_msgcount(port);
	n = offer->msgh_size + REQUESTED_TRAILER_SIZE(option);
	if (n > rcv_size) {
		n = rcv_size;
	}
	if (n > sizeof(save) || n == 0) {
		return;
	}
	machport_offer_rcv = offer;
	machport_offer_rcv_opt = option;
	machport_offer_rcv_size = rcv_size;
	machport_offer_rcv_n = n;
	machport_offer_rcv_port = port;
	if (!machport_stash34_has(port)) {
		fprintf(stderr,
		    "channelmac_dispatch_34_ahead_v1 skip inbound 20 after Offer pid=%d port=%u nmsg=%d stash34=0 offer_size=%u\n",
		    (int)getpid(), (unsigned)port, nmsg,
		    (unsigned)offer->msgh_size);
		fflush(stderr);
		machport_knote_log(
		    "channelmac_dispatch_34_ahead_v1 skip inbound 20 after Offer port=%u nmsg=%d",
		    (unsigned)port, nmsg);
		return;
	}
	for (i = 0; i < machport_watch_n; i++) {
		struct machport_watch *w = &machport_watches[i];

		if (!machport_watch_proven(w->sub, w->chan, w->ot_top, w->fn)) {
			continue;
		}
		if (w->port != MACH_PORT_NULL && w->port != port) {
			continue;
		}
		fprintf(stderr,
		    "channelmac_dispatch_34_ahead_v1 nest=0 after Offer pid=%d port=%u nmsg=%d this=%p sub=%p ot=%ld stash34=%d offer_size=%u\n",
		    (int)getpid(), (unsigned)port, nmsg, w->thisp, w->sub,
		    w->ot_top, machport_stash34_has(port), (unsigned)offer->msgh_size);
		fflush(stderr);
		machport_knote_log(
		    "channelmac_dispatch_34_ahead_v1 nest=0 after Offer port=%u nmsg=%d",
		    (unsigned)port, nmsg);
		memcpy(save, offer, n);
		machport_after_offer_busy = 1;
		w->fn(w->sub, port);
		memcpy(offer, save, n);
		machport_after_offer_busy = 0;
		fprintf(stderr,
		    "channelmac_dispatch_34_ahead_v1 nest=0 after Offer returned pid=%d port=%u nmsg=%d\n",
		    (int)getpid(), (unsigned)port, machport_msgcount(port));
		fflush(stderr);
		return;
	}
	fprintf(stderr,
	    "channelmac_dispatch_34_ahead_v1 nest=0 after Offer pid=%d port=%u nmsg=%d skip (no proven this=sub)\n",
	    (int)getpid(), (unsigned)port, nmsg);
	fflush(stderr);
}

static void
machport_dispatch_bind_before_send(void)
{
	int i;
	void *thisp;
	static int send_n;

	(void)machport_nested_timeout0_v1;
	(void)machport_dispatch_queue_v1;
	(void)machport_dispatch_34_ahead_v1;
	(void)machport_dispatch_34_skip_nest1_v1;
	if (machport_watch_n == 0) {
		if (send_n < 8) {
			send_n++;
			fprintf(stderr,
			    "channelmac_nested_timeout0_v1 send-only network pid=%d watches=0 skip (need knote capture)\n",
			    (int)getpid());
			fflush(stderr);
		}
		return;
	}
	thisp = machport_watches[0].thisp;
	if (send_n < 64) {
		send_n++;
		fprintf(stderr,
		    "channelmac_nested_timeout0_v1 send-only network pid=%d watches=%d this=%p maps_n=%d\n",
		    (int)getpid(), machport_watch_n, thisp, machport_maps_n);
		fflush(stderr);
	}
	for (i = 0; i < machport_watch_n; i++) {
		struct machport_watch *w = &machport_watches[i];
		void *this_arg;
		int nmsg;
		int drained;

		if (!machport_watch_proven(w->sub, w->chan, w->ot_top, w->fn)) {
			continue;
		}
		if (w->port == MACH_PORT_NULL) {
			continue;
		}
		/*
		 * Storage ProcessEvents: controller->watcher()->
		 * OnMachMessageReceived(port) is a virtual call on
		 * the MachPortWatcher subobject (this=sub). Passing
		 * ChannelMac* (chan=sub-88) SIGSEGV'd rdi=0 at the
		 * receive_buffer_ zero (1474114 rip=fn+0xe8).
		 * Dispatch only the proven ChannelMac slot
		 * (ot=-88, chan!=0, this=sub). Do not recapture
		 * on nest=1 recv.
		 *
		 * Drain only Bind (msgid 22). FIFO-drain of inbound
		 * msgid 20 during Connect/Accept send ate proto=4
		 * Ping (code229, nmsg 22→0). If a 20 is at the head,
		 * restore it and stop. Do not park Ping. Do not
		 * steal kmsgs. Do not drain 20s.
		 *
		 * code231: nested OnMachMessageReceived(34) during
		 * Accept/Ping send never returns. Do not call fn()
		 * for 34 on nest=1. Rotate keeps 34 at FIFO head
		 * for the nest=0 timeout-0 (storage path). Do not
		 * drain despite stash20 to hunt for 34.
		 */
		this_arg = w->sub;
		nmsg = machport_msgcount(w->port);
		drained = 0;
		machport_dispatch_stop_20 = 0;
		machport_dispatch_got_34 = 0;
		(void)machport_dispatch_no_drain20_v1;
		if (machport_restored_20_port == w->port ||
		    machport_stash34_has(w->port)) {
			fprintf(stderr,
			    "channelmac_dispatch_34_ahead_v1 skip nest=1 pid=%d port=%u nmsg=%d stash20=%d stash34=%d\n",
			    (int)getpid(), (unsigned)w->port, nmsg,
			    machport_restored_20_port == w->port,
			    machport_stash34_has(w->port));
			fflush(stderr);
			machport_knote_log(
			    "channelmac_dispatch_34_ahead_v1 skip nest=1 port=%u nmsg=%d",
			    (unsigned)w->port, nmsg);
			continue;
		}
		do {
			fprintf(stderr,
			    "channelmac_nested_timeout0_v1 dispatch Bind before send pid=%d this=%p sub=%p chan=%p this_arg=%p fn=%p thunk=%d ot=%ld port=%u nmsg=%d drain=%d\n",
			    (int)getpid(), w->thisp, w->sub, w->chan, this_arg,
			    (void *)w->fn, w->thunk, w->ot_top,
			    (unsigned)w->port, nmsg, drained);
			fflush(stderr);
			machport_knote_log(
			    "channelmac_nested_timeout0_v1 dispatch Bind before send port=%u nmsg=%d this_arg=%p thunk=%d drain=%d",
			    (unsigned)w->port, nmsg, this_arg, w->thunk, drained);
			(void)this_arg;
			machport_call_onmach(w);
			nmsg = machport_msgcount(w->port);
			drained++;
			fprintf(stderr,
			    "channelmac_nested_timeout0_v1 dispatch Bind returned pid=%d port=%u nmsg=%d drain=%d stop20=%d got34=%d\n",
			    (int)getpid(), (unsigned)w->port, nmsg, drained,
			    machport_dispatch_stop_20, machport_dispatch_got_34);
			fflush(stderr);
		} while (!machport_dispatch_stop_20 &&
		    !machport_dispatch_got_34 && nmsg > 0 && drained < 64);
		return;
	}
}

/*
 * Browser Channel: Connect/Bind sits un-copyout'd while IOThread
 * SENDs 20s, so helper Ping copyin beats browser 34. One nest=1
 * timeout-0 of sitting Connect (msgid=1) or Bind (msgid=22).
 * After Bind, leftover AcceptParcels Handle at nest=1 (this=sub).
 * Stash inbound 34 (no nested Handle(34)); do not stop leftover-20
 * on got34=1. Drain leftover 20s including proto=4 Ping until
 * nmsg=0. Dispatch nest=1 Ping (do not skip-restore). Unlock
 * write_lock_ only around Ping so Write(Pong) can mach_msg.
 * Other leftover 20s keep the lock. Restore inbound 20s only
 * during Connect/Accept. Dispatch this=sub only. Do not invent Pong.
 */
static void
machport_dispatch_connect_browser(void)
{
	int i;
	int nmsg;
	int drained;

	(void)machport_handshake_drain_recv_browser_v1;
	(void)machport_dispatch_34_browser_rot_v1;
	(void)machport_dispatch_skip20_after_bind_reverted_v1;
	(void)machport_leftover20_after_stash34_v1;
	(void)machport_pong_transmit_nest0_v1;
	if (machport_is_helper() || machport_is_gpu_process()) {
		return;
	}
	if (machport_watch_n == 0) {
		return;
	}
	for (i = 0; i < machport_watch_n; i++) {
		struct machport_watch *w = &machport_watches[i];

		if (!machport_watch_proven(w->sub, w->chan, w->ot_top, w->fn)) {
			continue;
		}
		if (w->port == MACH_PORT_NULL) {
			continue;
		}
		nmsg = machport_msgcount(w->port);
		if (nmsg < 1) {
			continue;
		}
		machport_dispatch_stop_20 = 0;
		machport_dispatch_got_34 = 0;
		if (machport_bind_done_has(w->port)) {
			(void)machport_skip_hs_ping_reverted_v1;
			drained = 0;
			machport_leftover_got_ping = 0;
			fprintf(stderr,
			    "skip leftover 20 after Bind reverted leftover 20s pid=%d this=%p sub=%p ot=%ld port=%u nmsg=%d stash34=%d\n",
			    (int)getpid(), w->thisp, w->sub, w->ot_top,
			    (unsigned)w->port, nmsg,
			    machport_stash34_has(w->port));
			fflush(stderr);
			machport_knote_log(
			    "skip leftover 20 after Bind reverted leftover 20s port=%u nmsg=%d",
			    (unsigned)w->port, nmsg);
			do {
				fprintf(stderr,
				    "handshake_drain_recv_v1 browser Channel leftover 20 after Bind pid=%d this=%p sub=%p ot=%ld port=%u nmsg=%d drain=%d\n",
				    (int)getpid(), w->thisp, w->sub, w->ot_top,
				    (unsigned)w->port, nmsg, drained);
				fflush(stderr);
				machport_call_onmach(w);
				nmsg = machport_msgcount(w->port);
				drained++;
				fprintf(stderr,
				    "handshake_drain_recv_v1 browser Channel leftover 20 after Bind returned pid=%d port=%u nmsg=%d drain=%d stop20=%d got34=%d ping=%d\n",
				    (int)getpid(), (unsigned)w->port, nmsg,
				    drained, machport_dispatch_stop_20,
				    machport_dispatch_got_34,
				    machport_leftover_got_ping);
				fflush(stderr);
				if (machport_dispatch_got_34) {
					fprintf(stderr,
					    "leftover 20 after Bind continue after stash34 pid=%d this=%p sub=%p ot=%ld port=%u nmsg=%d drain=%d stop20=%d\n",
					    (int)getpid(), w->thisp, w->sub,
					    w->ot_top, (unsigned)w->port, nmsg,
					    drained, machport_dispatch_stop_20);
					fflush(stderr);
					machport_knote_log(
					    "leftover 20 after Bind continue after stash34 port=%u nmsg=%d drain=%d",
					    (unsigned)w->port, nmsg, drained);
				}
			} while (!machport_dispatch_stop_20 &&
			    nmsg > 0 && drained < 64);
			fprintf(stderr,
			    "leftover 20 after Bind continue after stash34 done pid=%d port=%u nmsg=%d drain=%d got34=%d ping=%d this=sub=%p\n",
			    (int)getpid(), (unsigned)w->port, nmsg, drained,
			    machport_dispatch_got_34, machport_leftover_got_ping,
			    w->sub);
			fflush(stderr);
			return;
		}
		fprintf(stderr,
		    "handshake_drain_recv_v1 browser Channel dispatch Connect pid=%d this=%p sub=%p ot=%ld port=%u nmsg=%d\n",
		    (int)getpid(), w->thisp, w->sub, w->ot_top,
		    (unsigned)w->port, nmsg);
		fflush(stderr);
		machport_knote_log(
		    "handshake_drain_recv_v1 browser Channel dispatch Connect port=%u nmsg=%d",
		    (unsigned)w->port, nmsg);
		machport_call_onmach(w);
		fprintf(stderr,
		    "handshake_drain_recv_v1 browser Channel dispatch Connect returned pid=%d port=%u nmsg=%d stop20=%d got34=%d\n",
		    (int)getpid(), (unsigned)w->port, machport_msgcount(w->port),
		    machport_dispatch_stop_20, machport_dispatch_got_34);
		fflush(stderr);
		return;
	}
}

static mach_msg_return_t
machport_mach_msg_trap(mach_msg_header_t *msg, mach_msg_option_t option,
    mach_msg_size_t send_size, mach_msg_size_t rcv_size,
    mach_port_t rcv_name, mach_msg_timeout_t timeout, mach_port_t notify)
{
	mach_msg_return_t mr;
	mach_msg_option_t trap_opt;

	trap_opt = option & ~(MACH_SEND_INTERRUPT | MACH_RCV_INTERRUPT);
	mr = mach_msg_trap(msg, trap_opt, send_size, rcv_size, rcv_name,
	    timeout, notify);
	if (mr == MACH_MSG_SUCCESS) {
		return mr;
	}
	if ((option & MACH_SEND_INTERRUPT) == 0) {
		while (mr == MACH_SEND_INTERRUPTED) {
			mr = mach_msg_trap(msg, trap_opt, send_size, rcv_size,
			    rcv_name, timeout, notify);
		}
	}
	if ((option & MACH_RCV_INTERRUPT) == 0) {
		while (mr == MACH_RCV_INTERRUPTED) {
			mr = mach_msg_trap(msg,
			    trap_opt & ~MACH_SEND_MSG, 0, rcv_size,
			    rcv_name, timeout, notify);
		}
	}
	return mr;
}

/*
 * ChannelMac copyout order. Storage first recv is mjhs then MOJO.
 * Network leftover Offer sits at the head (offer-ahead-of-mjhs at
 * dserver post), so timeout-0 would copyout MOJO first and RLS stays
 * 0x0 at Ping. Handshake drain at this receive: if the head is MOJO
 * and mjhs is queued, return mjhs now and hold Offer for the next
 * recv. Do not hide Offer. Do not steal later kmsgs. FIFO otherwise.
 */
#define MACHPORT_HS_MOJO 0x4d4f4a4fu
#define MACHPORT_HS_MJHS 0x6d6a6873u
#define MACHPORT_HS_STASH_BYTES 8192
#define MACHPORT_HS_QMAX 32

struct machport_hs_stash {
	int valid;
	mach_port_t port;
	mach_msg_return_t kr;
	mach_msg_size_t nbytes;
	unsigned char buf[MACHPORT_HS_STASH_BYTES];
};

static struct machport_hs_stash machport_hs_q[MACHPORT_HS_QMAX];
static int machport_hs_q_n;
static mach_port_t machport_hs_mjhs_seen[8];
static int machport_hs_mjhs_n;

static int
machport_hs_got_mjhs(mach_port_t port)
{
	int i;

	for (i = 0; i < machport_hs_mjhs_n; i++) {
		if (machport_hs_mjhs_seen[i] == port) {
			return 1;
		}
	}
	return 0;
}

static void
machport_hs_mark_mjhs(mach_port_t port)
{
	if (machport_hs_got_mjhs(port)) {
		return;
	}
	if (machport_hs_mjhs_n >= 8) {
		return;
	}
	machport_hs_mjhs_seen[machport_hs_mjhs_n++] = port;
}

static mach_msg_size_t
machport_hs_nbytes(const mach_msg_header_t *msg, mach_msg_option_t option,
    mach_msg_size_t rcv_size)
{
	mach_msg_size_t n;

	if (msg == NULL || rcv_size == 0) {
		return 0;
	}
	n = msg->msgh_size;
	n += REQUESTED_TRAILER_SIZE(option);
	if (n > rcv_size) {
		n = rcv_size;
	}
	if (n > MACHPORT_HS_STASH_BYTES) {
		n = MACHPORT_HS_STASH_BYTES;
	}
	return n;
}

static int
machport_hs_push(mach_port_t port, mach_msg_header_t *msg,
    mach_msg_option_t option, mach_msg_size_t rcv_size,
    mach_msg_return_t kr)
{
	struct machport_hs_stash *s;
	mach_msg_size_t n;

	if (machport_hs_q_n >= MACHPORT_HS_QMAX || msg == NULL) {
		return 0;
	}
	n = machport_hs_nbytes(msg, option, rcv_size);
	if (n == 0) {
		return 0;
	}
	s = &machport_hs_q[machport_hs_q_n];
	memcpy(s->buf, msg, n);
	s->valid = 1;
	s->port = port;
	s->kr = kr;
	s->nbytes = n;
	machport_hs_q_n++;
	return 1;
}

static int
machport_hs_has(mach_port_t port)
{
	int i;

	for (i = 0; i < machport_hs_q_n; i++) {
		if (machport_hs_q[i].valid && machport_hs_q[i].port == port) {
			return 1;
		}
	}
	return 0;
}

static int
machport_hs_pop(mach_port_t port, mach_msg_header_t *msg,
    mach_msg_size_t rcv_size, mach_msg_return_t *kr)
{
	struct machport_hs_stash *s;
	int i;
	int j;

	if (machport_hs_q_n <= 0 || msg == NULL) {
		return 0;
	}
	s = NULL;
	for (i = 0; i < machport_hs_q_n; i++) {
		if (machport_hs_q[i].valid && machport_hs_q[i].port == port) {
			s = &machport_hs_q[i];
			break;
		}
	}
	if (s == NULL) {
		return 0;
	}
	if (s->nbytes > rcv_size) {
		return 0;
	}
	memcpy(msg, s->buf, s->nbytes);
	*kr = s->kr;
	for (j = i; j < machport_hs_q_n - 1; j++) {
		machport_hs_q[j] = machport_hs_q[j + 1];
	}
	machport_hs_q_n--;
	if (machport_hs_q_n == 0) {
		machport_hs_q[0].valid = 0;
	}
	return 1;
}

static unsigned
machport_hs_head_msgid(mach_port_t port)
{
	int i;

	for (i = 0; i < machport_hs_q_n; i++) {
		if (machport_hs_q[i].valid && machport_hs_q[i].port == port) {
			return machport_ipcz_msgid(
			    (const mach_msg_header_t *)machport_hs_q[i].buf);
		}
	}
	return 0;
}

/*
 * After Connect/Accept send returns, leftover Offer is still in
 * receive_buffer_. Peek timeout-0 (rotate may put 34 at head).
 * fn() only for 34. Restore inbound 20s without Handle. Do not
 * run this after Ping send (nested Handle during Ping hangs).
 */
static void
machport_dispatch_34_after_connect(void)
{
	int i;
	int nmsg;
	unsigned id;
	mach_msg_return_t pkr;
	mach_msg_option_t peek_opt;
	unsigned char save[8192];
	unsigned char peek[8192];
	mach_msg_header_t *ph;
	mach_msg_header_t *offer;
	mach_port_t port;

	(void)machport_dispatch_34_after_offer_v1;
	(void)machport_dispatch_skip20_after_offer_v1;
	if (machport_after_offer_busy || machport_in_dispatch) {
		return;
	}
	if (machport_offer_rcv == NULL || machport_is_gpu_process()) {
		return;
	}
	offer = machport_offer_rcv;
	port = machport_offer_rcv_port;
	nmsg = machport_msgcount(port);
	peek_opt = machport_offer_rcv_opt & ~(MACH_SEND_MSG);
	peek_opt |= MACH_RCV_MSG | MACH_RCV_TIMEOUT;
	memset(peek, 0, sizeof(peek));
	ph = (mach_msg_header_t *)peek;
	machport_after_offer_busy = 1;
	pkr = machport_mach_msg_trap(ph, peek_opt, 0, sizeof(peek), port,
	    0, MACH_PORT_NULL);
	if (pkr != MACH_MSG_SUCCESS) {
		machport_after_offer_busy = 0;
		fprintf(stderr,
		    "channelmac_dispatch_34_ahead_v1 nest=0 after Offer pid=%d port=%u nmsg=%d skip (empty) after_send kr=0x%x\n",
		    (int)getpid(), (unsigned)port, nmsg, (unsigned)pkr);
		fflush(stderr);
		return;
	}
	id = machport_ipcz_msgid(ph);
	if (id == 20) {
		if (machport_hs_push(port, ph, peek_opt, sizeof(peek), pkr)) {
			machport_skip_hs20_until_34 = 1;
		}
		machport_after_offer_busy = 0;
		machport_offer_rcv = NULL;
		fprintf(stderr,
		    "channelmac_dispatch_34_ahead_v1 skip inbound 20 after Offer pid=%d port=%u nmsg=%d size=%u after_send=1\n",
		    (int)getpid(), (unsigned)port, machport_msgcount(port),
		    (unsigned)ph->msgh_size);
		fflush(stderr);
		machport_knote_log(
		    "channelmac_dispatch_34_ahead_v1 skip inbound 20 after Offer port=%u after_send=1",
		    (unsigned)port);
		return;
	}
	if (id != 34) {
		(void)machport_hs_push(port, ph, peek_opt, sizeof(peek), pkr);
		machport_after_offer_busy = 0;
		machport_offer_rcv = NULL;
		fprintf(stderr,
		    "channelmac_dispatch_34_ahead_v1 nest=0 after Offer pid=%d port=%u skip inbound ipcz=%u after_send=1\n",
		    (int)getpid(), (unsigned)port, id);
		fflush(stderr);
		return;
	}
	if (!machport_stash34_push(port, ph, peek_opt, sizeof(peek), pkr)) {
		(void)machport_hs_push(port, ph, peek_opt, sizeof(peek), pkr);
		machport_after_offer_busy = 0;
		fprintf(stderr,
		    "channelmac_dispatch_34_ahead_v1 nest=0 after Offer pid=%d port=%u stash-full after_send=1\n",
		    (int)getpid(), (unsigned)port);
		fflush(stderr);
		return;
	}
	for (i = 0; i < machport_watch_n; i++) {
		struct machport_watch *w = &machport_watches[i];

		if (!machport_watch_proven(w->sub, w->chan, w->ot_top, w->fn)) {
			continue;
		}
		if (w->port != MACH_PORT_NULL && w->port != port) {
			continue;
		}
		fprintf(stderr,
		    "channelmac_dispatch_34_ahead_v1 nest=0 after Offer pid=%d port=%u nmsg=%d this=%p sub=%p ot=%ld stash34=1 offer_size=%u after_send=1\n",
		    (int)getpid(), (unsigned)port, nmsg, w->thisp, w->sub,
		    w->ot_top, (unsigned)offer->msgh_size);
		fflush(stderr);
		memcpy(save, offer, machport_offer_rcv_n);
		w->fn(w->sub, port);
		memcpy(offer, save, machport_offer_rcv_n);
		machport_after_offer_busy = 0;
		machport_skip_hs20_until_34 = 0;
		machport_offer_rcv = NULL;
		fprintf(stderr,
		    "channelmac_dispatch_34_ahead_v1 nest=0 after Offer returned pid=%d port=%u nmsg=%d after_send=1\n",
		    (int)getpid(), (unsigned)port, machport_msgcount(port));
		fflush(stderr);
		return;
	}
	machport_after_offer_busy = 0;
	fprintf(stderr,
	    "channelmac_dispatch_34_ahead_v1 nest=0 after Offer pid=%d port=%u skip (no proven this=sub) after_send=1\n",
	    (int)getpid(), (unsigned)port);
	fflush(stderr);
}

static void
machport_sl1424_name_note(const char *dir, mach_msg_header_t *msg,
    mach_port_t port, mach_msg_return_t kr)
{
	static int n;
	static int n15;
	static int portal_dump;
	uint64_t sl;
	uint32_t iid;
	uint32_t name;
	uint32_t flags;
	uint32_t nbytes;
	unsigned msgid;
	unsigned seq;
	int is_sl1424;
	int is_name0;
	int helper_to_browser;

	(void)machport_helper_sl1424_name_v1;
	if (msg == NULL || machport_is_gpu_process()) {
		return;
	}
	msgid = machport_ipcz_msgid(msg);
	if (msgid != 20) {
		return;
	}
	sl = machport_ipcz_sublink20(msg);
	seq = machport_ipcz_proto20(msg);
	nbytes = machport_ipcz_num_bytes(msg);
	iid = 0xffffffffu;
	name = 0xffffffffu;
	flags = 0xffffffffu;
	(void)machport_mojo_hdr(msg, &iid, &name, &flags);
	is_sl1424 = (sl == 14 || sl == 34 || sl == 24) &&
	    (flags & 1u) != 0;
	is_name0 = (iid == 0 && name == 0 && (flags & 1u) != 0);
	if (sl == 15 && n15 < 16) {
		n15++;
		fprintf(stderr,
		    "helper_sl1424_name_v1 sl15 dir=%s pid=%d port=%u sl=15 seq=%u nb=%u iid=%u name=%u flags=%u nest=%d kr=0x%x net=%d stor=%d browser=%d\n",
		    dir, (int)getpid(), (unsigned)port, seq, nbytes, iid, name,
		    flags, machport_in_dispatch, (unsigned)kr,
		    machport_is_network(), machport_is_storage(),
		    !machport_is_helper() && !machport_is_gpu_process());
		fflush(stderr);
	}
	if (is_name0) {
		machport_childproc_bound = 1;
		machport_invite_trap_dump("name0", port);
	}
	if (!is_sl1424 && !is_name0) {
		return;
	}
	if (n >= 64) {
		return;
	}
	n++;
	helper_to_browser = 0;
	if (machport_is_helper() &&
	    (dir == NULL || strncmp(dir, "recv", 4) != 0)) {
		helper_to_browser = 1;
	}
	if (!machport_is_helper() && !machport_is_gpu_process() &&
	    dir != NULL && strncmp(dir, "recv", 4) == 0) {
		helper_to_browser = 1;
	}
	fprintf(stderr,
	    "helper_sl1424_name_v1 dir=%s pid=%d port=%u sl=%llu seq=%u nb=%u iid=%u name=%u flags=%u nest=%d kr=0x%x net=%d stor=%d browser=%d helper_to_browser=%d name0=%d\n",
	    dir, (int)getpid(), (unsigned)port, (unsigned long long)sl, seq,
	    nbytes, iid, name, flags, machport_in_dispatch, (unsigned)kr,
	    machport_is_network(), machport_is_storage(),
	    !machport_is_helper() && !machport_is_gpu_process(),
	    helper_to_browser, is_name0);
	fflush(stderr);
	machport_knote_log(
	    "helper_sl1424_name_v1 dir=%s sl=%llu name=%u flags=%u name0=%d nest=%d",
	    dir, (unsigned long long)sl, name, flags, is_name0,
	    machport_in_dispatch);
	if (!is_name0 && is_sl1424 && machport_is_helper() &&
	    portal_dump == 0) {
		portal_dump = 1;
		machport_helper_getrouter23_note(port, 15);
	}
}

static void
machport_real_ping_note(const char *dir, mach_msg_header_t *msg,
    mach_port_t port, mach_msg_return_t kr)
{
	static int n;
	uint64_t sl;
	uint32_t iid;
	uint32_t name;
	uint32_t flags;
	uint32_t nbytes;
	uint32_t ch_nb;
	uint16_t cht;
	uint16_t hdrb;
	unsigned msgid;
	unsigned seq;
	int interesting;
	int is_chan_ping;
	int is_sl15;
	int is_host_ping;

	(void)machport_helper_real_ping_v1;
	if (n >= 80 || msg == NULL || machport_is_gpu_process()) {
		return;
	}
	msgid = machport_ipcz_msgid(msg);
	sl = machport_ipcz_sublink20(msg);
	seq = machport_ipcz_proto20(msg);
	nbytes = machport_ipcz_num_bytes(msg);
	iid = 0xffffffffu;
	name = 0xffffffffu;
	flags = 0xffffffffu;
	if (msgid == 20) {
		(void)machport_mojo_hdr(msg, &iid, &name, &flags);
	}
	cht = machport_msg_ch_type(msg, &hdrb, &ch_nb);
	is_chan_ping = (cht == 2 || cht == 3);
	is_sl15 = (msgid == 20 && sl == 15 && (flags & 1u) != 0);
	is_host_ping = (msgid == 20 && iid == 0 && name == 0 &&
	    (flags & 1u) != 0);
	interesting = is_chan_ping || is_sl15 || is_host_ping;
	if (!interesting) {
		return;
	}
	n++;
	fprintf(stderr,
	    "helper_real_ping_v1 dir=%s pid=%d port=%u msgid=%u sl=%llu seq=%u nb=%u iid=%u name=%u flags=%u ch_type=%u ch_hdrb=%u ch_nb=%u kr=0x%x nest=%d net=%d stor=%d browser=%d chan_ping=%d sl15=%d host_ping=%d\n",
	    dir, (int)getpid(), (unsigned)port, msgid,
	    (unsigned long long)sl, seq, nbytes, iid, name, flags, cht, hdrb,
	    ch_nb, (unsigned)kr, machport_in_dispatch, machport_is_network(),
	    machport_is_storage(),
	    !machport_is_helper() && !machport_is_gpu_process(),
	    is_chan_ping, is_sl15, is_host_ping);
	fflush(stderr);
	machport_knote_log(
	    "helper_real_ping_v1 dir=%s port=%u sl=%llu nb=%u iid=%u name=%u flags=%u ch_type=%u",
	    dir, (unsigned)port, (unsigned long long)sl, nbytes, iid, name,
	    flags, cht);
}

static void
machport_send_note(const char *tag, mach_msg_header_t *msg,
    mach_msg_option_t option, mach_msg_size_t send_size,
    mach_msg_return_t mr)
{
	static int n;
	unsigned msgid;
	unsigned proto;
	unsigned nbytes;
	uint32_t iid;
	uint32_t mojo_flags;
	int pong_like;

	(void)machport_pong_transmit_nest0_v1;
	(void)machport_samechan_bind_seq4_v1;
	if (msg == NULL || (option & MACH_SEND_MSG) == 0 || send_size == 0) {
		return;
	}
	if (!machport_is_gpu_process()) {
		machport_sl1424_name_note(tag, msg, msg->msgh_remote_port, mr);
		machport_real_ping_note(tag, msg, msg->msgh_remote_port, mr);
	}
	msgid = machport_ipcz_msgid(msg);
	proto = machport_ipcz_proto20(msg);
	nbytes = machport_ipcz_num_bytes(msg);
	iid = 0xffffffffu;
	mojo_flags = 0xffffffffu;
	if ((machport_is_network() || machport_is_storage()) && msgid == 34) {
		machport_helper_34_note("send", msg, msg->msgh_remote_port);
	}
	if (msgid == 20) {
		(void)machport_mojo_from_msg(msg, &iid, &mojo_flags);
	}
	pong_like = (nbytes > 0 && nbytes < 64 && msgid == 20 &&
	    iid == 0 && (mojo_flags & 2u) != 0);
	if (pong_like && (machport_is_network() || machport_is_storage())) {
		(void)machport_helper_pong_send_v1;
		fprintf(stderr,
		    "helper_pong_send_v1 pid=%d dest=0x%x nb=%u iid=%u flags=%u sl=%llu seq=%u kr=0x%x nest=%d send_size=%u tag=%s net=%d stor=%d\n",
		    (int)getpid(), (unsigned)msg->msgh_remote_port, nbytes,
		    iid, mojo_flags,
		    (unsigned long long)machport_ipcz_sublink20(msg), proto,
		    (unsigned)mr, machport_in_dispatch, (unsigned)send_size,
		    tag, machport_is_network(), machport_is_storage());
		fflush(stderr);
		machport_knote_log(
		    "helper_pong_send_v1 dest=0x%x nb=%u flags=%u kr=0x%x nest=%d",
		    (unsigned)msg->msgh_remote_port, nbytes, mojo_flags,
		    (unsigned)mr, machport_in_dispatch);
	}
	if (pong_like) {
		fprintf(stderr,
		    "handle_order_v1 Pong copyout_attempt pid=%d dest=0x%x nb=%u iid=%u flags=%u msgid=%u sl=%llu seq=%u kr=0x%x nest=%d tag=%s send_size=%u\n",
		    (int)getpid(), (unsigned)msg->msgh_remote_port, nbytes,
		    iid, mojo_flags, msgid,
		    (unsigned long long)machport_ipcz_sublink20(msg), proto,
		    (unsigned)mr, machport_in_dispatch, tag,
		    (unsigned)send_size);
		fflush(stderr);
		machport_knote_log(
		    "handle_order_v1 Pong copyout_attempt dest=0x%x nb=%u flags=%u kr=0x%x nest=%d",
		    (unsigned)msg->msgh_remote_port, nbytes, mojo_flags,
		    (unsigned)mr, machport_in_dispatch);
	}
	if (machport_in_dispatch && proto == 4) {
		(void)machport_unlock_around_nested_pong_v1;
		fprintf(stderr,
		    "channelmac_unlock_around_callback_v1 nested proto=4 send pid=%d dest=0x%x num_bytes=%u send_size=%u kr=0x%x tag=%s nest=%d msgid=%u\n",
		    (int)getpid(), (unsigned)msg->msgh_remote_port, nbytes,
		    (unsigned)send_size, (unsigned)mr, tag,
		    machport_in_dispatch, msgid);
		fflush(stderr);
	}
	if (machport_ping_wait_send) {
		machport_ping_wait_sends++;
		fprintf(stderr,
		    "channelmac_parcel_dump_v1 send_after_nest0_ping pid=%d dest=0x%x send_size=%u msgid=%u proto=%u num_bytes=%u kr=0x%x nest=%d n=%d wait_port=%u tag=%s pong_like=%d\n",
		    (int)getpid(), (unsigned)msg->msgh_remote_port,
		    (unsigned)send_size, msgid, proto, nbytes, (unsigned)mr,
		    machport_in_dispatch, machport_ping_wait_sends,
		    machport_ping_wait_port, tag, pong_like);
		fflush(stderr);
	}
	if (n >= 64) {
		return;
	}
	n++;
	fprintf(stderr,
	    "pong_transmit_nest0_v1 %s pid=%d nest=%d send_size=%u dest=0x%x msgid=%u proto=%u num_bytes=%u kr=0x%x opt=0x%x\n",
	    tag, (int)getpid(), machport_in_dispatch, (unsigned)send_size,
	    (unsigned)msg->msgh_remote_port, msgid, proto, nbytes,
	    (unsigned)mr, (unsigned)option);
	fflush(stderr);
	if (machport_ping_wait_send) {
		machport_ping_wait_sends++;
		fprintf(stderr,
		    "channelmac_parcel_dump_v1 send_after_nest0_ping pid=%d dest=0x%x send_size=%u msgid=%u proto=%u num_bytes=%u kr=0x%x nest=%d n=%d wait_port=%u pong_like=%d\n",
		    (int)getpid(), (unsigned)msg->msgh_remote_port,
		    (unsigned)send_size, msgid, proto, nbytes, (unsigned)mr,
		    machport_in_dispatch, machport_ping_wait_sends,
		    machport_ping_wait_port, pong_like);
		fflush(stderr);
	}
}

static mach_msg_return_t
machport_mach_msg(mach_msg_header_t *msg, mach_msg_option_t option,
    mach_msg_size_t send_size, mach_msg_size_t rcv_size,
    mach_port_t rcv_name, mach_msg_timeout_t timeout, mach_port_t notify)
{
	void *ret0;
	mach_msg_return_t mr;
	int is_net;
	int is_stor;
	int is_browser;
	int is_watch_rcv;
	int drained;
	static int send_ret_n;
	static int recv_n;

	is_net = machport_is_network() && !machport_is_gpu_process();
	is_stor = machport_is_storage() && !machport_is_gpu_process();
	is_browser = !machport_is_helper() && !machport_is_gpu_process();
	is_watch_rcv = ((option & MACH_RCV_MSG) != 0 &&
	    (option & MACH_RCV_TIMEOUT) != 0 &&
	    timeout == 0 && send_size == 0 &&
	    GET_RCV_ELEMENTS(option) == MACH_RCV_TRAILER_AUDIT);
	drained = 0;
	if (machport_ping_wait_send && is_watch_rcv) {
		fprintf(stderr,
		    "channelmac_parcel_dump_v1 after_handle next_recv pid=%d port=%u send_n=%d nest=%d wait_port=%u\n",
		    (int)getpid(), (unsigned)rcv_name, machport_ping_wait_sends,
		    machport_in_dispatch, machport_ping_wait_port);
		fflush(stderr);
		machport_ping_wait_send = 0;
	}
	if (is_net) {
		if ((option & MACH_SEND_MSG) != 0 &&
		    (option & MACH_SEND_TIMEOUT) != 0 &&
		    (option & MACH_RCV_MSG) == 0 &&
		    timeout == 0 && rcv_size == 0 && send_ret_n < 4) {
			send_ret_n++;
			ret0 = __builtin_return_address(0);
			machport_maps_refresh();
			fprintf(stderr,
			    "channelmac_nested_timeout0_v1 comet frame pid=%d ret0=%p comet_rx=%d watches=%d\n",
			    (int)getpid(), ret0, machport_maps_code(ret0),
			    machport_watch_n);
			fflush(stderr);
			machport_log_addr_map("send_ret0", ret0);
		}
		if (is_watch_rcv) {
			machport_maps_refresh();
			if (!machport_in_dispatch) {
				machport_watch_store(machport_caller_this(),
				    rcv_name);
			} else {
				(void)machport_no_nest1_recapture_v1;
				fprintf(stderr,
				    "channelmac_no_nest1_recapture_v1 skip recv pid=%d port=%u nest=1\n",
				    (int)getpid(), (unsigned)rcv_name);
				fflush(stderr);
			}
			/*
			 * Handshake stash is the leftover Offer for
			 * nest=0 Connect. Nest-1 dispatch during send
			 * must not pop it (nmsg=0 would OnOffer during
			 * Connect and Bind never posts). Kernel FIFO
			 * only on nest=1 so lagged msgid 34 still
			 * copyin before Ping.
			 *
			 * msgid 34 stashed on nest=1 (code231 hang)
			 * is delivered on nest=0 timeout-0 only
			 * (storage path). Rotate keeps 34 at head
			 * when it was never dequeued.
			 */
			if (!machport_in_dispatch &&
			    machport_stash34_pop(rcv_name, msg, rcv_size,
				&mr)) {
				(void)machport_dispatch_34_nest0_v1;
				drained = 1;
				machport_skip_hs20_until_34 = 0;
				machport_offer_rcv = NULL;
				fprintf(stderr,
				    "channelmac_dispatch_34_ahead_v1 nest=0 msgid=34 pid=%d port=%u size=%u stash=1\n",
				    (int)getpid(), (unsigned)rcv_name,
				    (unsigned)msg->msgh_size);
				fflush(stderr);
				machport_knote_log(
				    "channelmac_dispatch_34_ahead_v1 nest=0 msgid=34 port=%u size=%u",
				    (unsigned)rcv_name,
				    (unsigned)msg->msgh_size);
				goto recv_logged;
			}
			if (!machport_in_dispatch &&
			    !(machport_skip_hs20_until_34 &&
			      machport_hs_head_msgid(rcv_name) == 20) &&
			    machport_hs_pop(rcv_name, msg, rcv_size, &mr)) {
				if (machport_restored_20_port == rcv_name &&
				    machport_ipcz_msgid(msg) == 20) {
					machport_restored_20_port = MACH_PORT_NULL;
				}
				if (mr == MACH_MSG_SUCCESS && msg != NULL &&
				    msg->msgh_id == (mach_msg_id_t)MACHPORT_HS_MJHS) {
					machport_hs_mark_mjhs(rcv_name);
				}
				drained = 1;
				if (mr == MACH_MSG_SUCCESS &&
				    machport_hs_got_mjhs(rcv_name) &&
				    machport_is_offer_copyout(msg)) {
					machport_dispatch_34_after_offer(rcv_name,
					    msg, option, rcv_size);
				}
				goto recv_logged;
			}
		}
		if (!machport_in_dispatch &&
		    (option & MACH_SEND_MSG) != 0 &&
		    (option & MACH_SEND_TIMEOUT) != 0 &&
		    (option & MACH_RCV_MSG) == 0 &&
		    timeout == 0 && rcv_size == 0) {
			unsigned char save[8192];
			mach_msg_size_t nsave;

			nsave = send_size;
			if (nsave > sizeof(save)) {
				nsave = sizeof(save);
			}
			if (msg != NULL && nsave > 0) {
				memcpy(save, msg, nsave);
			} else {
				nsave = 0;
			}
			machport_in_dispatch = 1;
			machport_dispatch_bind_before_send();
			machport_in_dispatch = 0;
			if (msg != NULL && nsave > 0) {
				memcpy(msg, save, nsave);
			}
		}
	} else if ((is_stor || is_browser) && is_watch_rcv) {
		machport_maps_refresh();
		if (!machport_in_dispatch) {
			machport_watch_store(machport_caller_this(), rcv_name);
		}
		/*
		 * Nest=0 timeout-0: restored Ping (hs_q) before
		 * stashed 34. FIFO. Do not rotate-34. Do not
		 * nested Handle(34). Do not drain 20s.
		 */
		if (is_browser && !machport_in_dispatch &&
		    machport_hs_pop(rcv_name, msg, rcv_size, &mr)) {
			(void)machport_pong_transmit_nest0_v1;
			drained = 1;
			fprintf(stderr,
			    "pong_transmit_nest0_v1 nest=0 Ping pid=%d port=%u size=%u proto=%u stash=1\n",
			    (int)getpid(), (unsigned)rcv_name,
			    (unsigned)msg->msgh_size,
			    machport_ipcz_proto20(msg));
			fflush(stderr);
			machport_knote_log(
			    "pong_transmit_nest0_v1 nest=0 Ping port=%u size=%u proto=%u",
			    (unsigned)rcv_name, (unsigned)msg->msgh_size,
			    machport_ipcz_proto20(msg));
			goto recv_logged;
		}
		if (is_browser && !machport_in_dispatch &&
		    machport_stash34_pop(rcv_name, msg, rcv_size, &mr)) {
			(void)machport_dispatch_34_nest0_v1;
			drained = 1;
			fprintf(stderr,
			    "channelmac_dispatch_34_ahead_v1 nest=0 msgid=34 pid=%d port=%u size=%u stash=1\n",
			    (int)getpid(), (unsigned)rcv_name,
			    (unsigned)msg->msgh_size);
			fflush(stderr);
			machport_knote_log(
			    "channelmac_dispatch_34_ahead_v1 nest=0 msgid=34 port=%u size=%u",
			    (unsigned)rcv_name, (unsigned)msg->msgh_size);
			goto recv_logged;
		}
	}
	if (is_browser &&
	    !machport_in_dispatch &&
	    (option & MACH_SEND_MSG) != 0 &&
	    (option & MACH_SEND_TIMEOUT) != 0 &&
	    (option & MACH_RCV_MSG) == 0 &&
	    timeout == 0 && rcv_size == 0) {
		unsigned char save[8192];
		mach_msg_size_t nsave;

		nsave = send_size;
		if (nsave > sizeof(save)) {
			nsave = sizeof(save);
		}
		if (msg != NULL && nsave > 0) {
			memcpy(save, msg, nsave);
		} else {
			nsave = 0;
		}
		machport_in_dispatch = 1;
		machport_send_note("leftover send-trap before", msg, option,
		    send_size, (mach_msg_return_t)0xffffffff);
		machport_dispatch_connect_browser();
		machport_in_dispatch = 0;
		if (msg != NULL && nsave > 0) {
			memcpy(msg, save, nsave);
		}
	}
	if (is_browser && (option & MACH_SEND_MSG) != 0 && send_size > 0) {
		machport_send_note("copyout_attempt", msg, option, send_size,
		    (mach_msg_return_t)0xfffffffe);
	}
	if ((is_net || is_stor) && (option & MACH_SEND_MSG) != 0 &&
	    send_size > 0 && msg != NULL) {
		machport_sl1424_name_note("send", msg, msg->msgh_remote_port,
		    (mach_msg_return_t)0xfffffffe);
		machport_real_ping_note("send", msg, msg->msgh_remote_port,
		    (mach_msg_return_t)0xfffffffe);
	}
	mr = machport_mach_msg_trap(msg, option, send_size, rcv_size,
	    rcv_name, timeout, notify);
	if (is_browser && (option & MACH_SEND_MSG) != 0 && send_size > 0) {
		machport_send_note("copyout_kr", msg, option, send_size, mr);
	}
	if ((is_net || is_stor) && (option & MACH_SEND_MSG) != 0 &&
	    send_size > 0 && msg != NULL) {
		unsigned imsg;

		machport_sl1424_name_note("send_kr", msg, msg->msgh_remote_port,
		    mr);
		machport_real_ping_note("send_kr", msg, msg->msgh_remote_port,
		    mr);
		imsg = machport_ipcz_msgid(msg);
		if (imsg == 1) {
			machport_invite_trap_dump("connect1_send",
			    rcv_name);
		} else if (imsg == 22) {
			machport_invite_trap_dump("bind22_send", rcv_name);
		} else if (imsg == 0) {
			machport_invite_trap_dump("connect0_send", rcv_name);
		}
	}
	if (machport_is_helper() && (option & MACH_SEND_MSG) != 0 &&
	    send_size > 0 && msg != NULL && !is_net && !is_stor) {
		unsigned imsg = machport_ipcz_msgid(msg);

		if (imsg == 0 || imsg == 1 || imsg == 22) {
			machport_invite_trap_dump(
			    imsg == 22 ? "bind22_send" :
			    (imsg == 1 ? "connect1_send" : "connect0_send"),
			    rcv_name);
		}
	}
	if (is_browser && is_watch_rcv && mr == MACH_MSG_SUCCESS &&
	    msg != NULL && machport_ipcz_msgid(msg) == 22) {
		machport_bind_done_set(rcv_name);
	}
	if (is_net &&
	    (option & MACH_SEND_MSG) != 0 &&
	    (option & MACH_SEND_TIMEOUT) != 0 &&
	    (option & MACH_RCV_MSG) == 0 &&
	    timeout == 0 && rcv_size == 0 &&
	    !machport_in_dispatch &&
	    machport_offer_rcv != NULL) {
		if (machport_ipcz_msgid(msg) == 20 &&
		    machport_ipcz_proto20(msg) == 4) {
			machport_offer_rcv = NULL;
		} else if (mr == MACH_MSG_SUCCESS) {
			machport_dispatch_34_after_connect();
		}
	}
	if ((is_net || is_browser) && is_watch_rcv && machport_in_dispatch &&
	    mr == MACH_MSG_SUCCESS && msg != NULL &&
	    machport_ipcz_msgid(msg) == 20) {
		if (is_browser && machport_bind_done_has(rcv_name)) {
			uint32_t ping_iid = 0xffffffffu;
			uint32_t ping_name = 0xffffffffu;
			uint32_t ping_flags = 0xffffffffu;

			(void)machport_mojo_hdr(msg, &ping_iid, &ping_name,
			    &ping_flags);
			/*
			 * Real ChildProcess.Ping (name=0) at nest=1.
			 * Fable: nest=0 lock-free Handle on
			 * Chrome_IOThread. Do not nested Handle.
			 * Do not unlock-around-callback as the fix.
			 */
			if (ping_iid == 0 && ping_name == 0 &&
			    (ping_flags & 1u) != 0 &&
			    machport_hs_push(rcv_name, msg, option,
			    rcv_size, mr)) {
				fprintf(stderr,
				    "helper_sl1424_name_v1 nest=1 name=0 skip pid=%d port=%u sl=%llu seq=%u nest=0 pending Chrome_IOThread\n",
				    (int)getpid(), (unsigned)rcv_name,
				    (unsigned long long)machport_ipcz_sublink20(msg),
				    machport_ipcz_proto20(msg));
				fflush(stderr);
				mr = MACH_RCV_TIMED_OUT;
				goto recv_logged;
			}
			/*
			 * After Bind, leftover 20s Handle at nest=1
			 * (send-trap, ChannelMac write_lock_ held).
			 * Unlock write_lock_ only for proto=4 leftover
			 * so Write can mach_msg. AcceptParcels keep
			 * the lock (code245 992). Leftover-20 still
			 * continues after stash34.
			 */
			if (machport_ipcz_proto20(msg) == 4) {
				machport_unlock_for_ping(rcv_name);
			}
			goto recv_logged;
		}
		/*
		 * Nest-1 dispatch during Connect/Accept send must
		 * not FIFO-drain inbound msgid 20 (Accept/Ping).
		 * Restore the kmsg and stop; nest=0 watcher copyin
		 * keeps FIFO. Do not park Ping. Do not steal.
		 */
		(void)machport_dispatch_no_drain20_v1;
		if (machport_hs_push(rcv_name, msg, option, rcv_size, mr)) {
			machport_dispatch_stop_20 = 1;
			machport_restored_20_port = rcv_name;
			fprintf(stderr,
			    "channelmac_dispatch_no_drain20_v1 skip msgid=20 pid=%d port=%u size=%u nest=1\n",
			    (int)getpid(), (unsigned)rcv_name,
			    (unsigned)msg->msgh_size);
			fflush(stderr);
			machport_knote_log(
			    "channelmac_dispatch_no_drain20_v1 skip msgid=20 port=%u size=%u",
			    (unsigned)rcv_name, (unsigned)msg->msgh_size);
			mr = MACH_RCV_TIMED_OUT;
			goto recv_logged;
		}
		machport_dispatch_stop_20 = 1;
		fprintf(stderr,
		    "channelmac_dispatch_no_drain20_v1 stash-full msgid=20 pid=%d port=%u\n",
		    (int)getpid(), (unsigned)rcv_name);
		fflush(stderr);
		/*
		 * Do not drain 20s when stash is full. TIMED_OUT so
		 * nested Handle does not FIFO-drain Ping/Accept.
		 * Per-port hs_q (QMAX 32) should have taken it.
		 */
		mr = MACH_RCV_TIMED_OUT;
		goto recv_logged;
	}
	if ((is_net || is_stor) && is_watch_rcv &&
	    mr == MACH_MSG_SUCCESS && msg != NULL &&
	    machport_ipcz_msgid(msg) == 34) {
		machport_helper_34_note("recv", msg, rcv_name);
	}
	if ((is_net || is_browser) && is_watch_rcv && machport_in_dispatch &&
	    mr == MACH_MSG_SUCCESS && msg != NULL &&
	    machport_ipcz_msgid(msg) == 34) {
		/*
		 * code231: SUCCESS 34 into nested OnMachMessageReceived
		 * never returns. Stash and TIMED_OUT so fn() returns.
		 * Nest=0 timeout-0 pops this (storage path). Do not
		 * Handle 34 while Send is on the stack.
		 */
		(void)machport_dispatch_34_ahead_v1;
		(void)machport_dispatch_34_skip_nest1_v1;
		machport_dispatch_got_34 = 1;
		if (machport_stash34_push(rcv_name, msg, option, rcv_size,
		    mr)) {
			fprintf(stderr,
			    "channelmac_dispatch_34_ahead_v1 skip nest=1 msgid=34 pid=%d port=%u size=%u\n",
			    (int)getpid(), (unsigned)rcv_name,
			    (unsigned)msg->msgh_size);
			fflush(stderr);
			machport_knote_log(
			    "channelmac_dispatch_34_ahead_v1 skip nest=1 msgid=34 port=%u size=%u",
			    (unsigned)rcv_name, (unsigned)msg->msgh_size);
			mr = MACH_RCV_TIMED_OUT;
			goto recv_logged;
		}
		fprintf(stderr,
		    "channelmac_dispatch_34_ahead_v1 nest=1 msgid=34 stash-full pid=%d port=%u\n",
		    (int)getpid(), (unsigned)rcv_name);
		fflush(stderr);
		/*
		 * Do not nested Handle(34) when stash is full.
		 * TIMED_OUT so Connect send can finish and post
		 * sl=2 34. Per-port stash34 should have taken it.
		 */
		mr = MACH_RCV_TIMED_OUT;
		goto recv_logged;
	}
	if (is_net && is_watch_rcv && !machport_in_dispatch &&
	    mr == MACH_MSG_SUCCESS && msg != NULL &&
	    machport_ipcz_msgid(msg) == 34) {
		(void)machport_dispatch_34_nest0_v1;
		machport_skip_hs20_until_34 = 0;
		machport_offer_rcv = NULL;
		fprintf(stderr,
		    "channelmac_dispatch_34_ahead_v1 nest=0 msgid=34 pid=%d port=%u size=%u stash=0\n",
		    (int)getpid(), (unsigned)rcv_name,
		    (unsigned)msg->msgh_size);
		fflush(stderr);
		machport_knote_log(
		    "channelmac_dispatch_34_ahead_v1 nest=0 msgid=34 port=%u size=%u",
		    (unsigned)rcv_name, (unsigned)msg->msgh_size);
	}
	if (is_net && is_watch_rcv && !machport_in_dispatch &&
	    mr == MACH_MSG_SUCCESS && msg != NULL &&
	    msg->msgh_id == (mach_msg_id_t)MACHPORT_HS_MOJO &&
	    !machport_hs_got_mjhs(rcv_name) &&
	    machport_msgcount(rcv_name) >= 1 &&
	    !machport_hs_has(rcv_name) &&
	    machport_hs_push(rcv_name, msg, option, rcv_size, mr)) {
		mach_msg_return_t mr2;

		mr2 = machport_mach_msg_trap(msg, option, 0, rcv_size,
		    rcv_name, timeout, notify);
		if (mr2 == MACH_MSG_SUCCESS && msg != NULL &&
		    msg->msgh_id == (mach_msg_id_t)MACHPORT_HS_MJHS) {
			(void)machport_handshake_drain_recv_v1;
			machport_hs_mark_mjhs(rcv_name);
			fprintf(stderr,
			    "handshake_drain_recv_v1 mjhs-then-MOJO pid=%d port=%u mjhs_size=%u mojo_size=%u nest=%d\n",
			    (int)getpid(), (unsigned)rcv_name,
			    (unsigned)msg->msgh_size,
			    (unsigned)machport_hs_q[0].nbytes, machport_in_dispatch);
			fflush(stderr);
			machport_knote_log(
			    "handshake_drain_recv_v1 mjhs-then-MOJO port=%u mjhs_size=%u",
			    (unsigned)rcv_name, (unsigned)msg->msgh_size);
			mr = mr2;
		} else if (mr2 == MACH_MSG_SUCCESS) {
			/*
			 * Next was not mjhs. Keep FIFO: this recv is
			 * the original MOJO; held second follows.
			 */
			if (!machport_hs_push(rcv_name, msg, option,
			    rcv_size, mr2) ||
			    !machport_hs_pop(rcv_name, msg, rcv_size, &mr)) {
				machport_knote_log(
				    "handshake_drain_recv_v1 fifo-restore fail port=%u",
				    (unsigned)rcv_name);
			}
		} else if (!machport_hs_pop(rcv_name, msg, rcv_size, &mr)) {
			machport_knote_log(
			    "handshake_drain_recv_v1 restore fail port=%u kr2=0x%x",
			    (unsigned)rcv_name, (unsigned)mr2);
		}
	} else if (is_net && is_watch_rcv && mr == MACH_MSG_SUCCESS &&
	    msg != NULL &&
	    msg->msgh_id == (mach_msg_id_t)MACHPORT_HS_MJHS) {
		machport_hs_mark_mjhs(rcv_name);
	}
	if (is_net && is_watch_rcv && !machport_in_dispatch &&
	    !machport_after_offer_busy &&
	    mr == MACH_MSG_SUCCESS &&
	    machport_hs_got_mjhs(rcv_name) &&
	    machport_is_offer_copyout(msg)) {
		machport_dispatch_34_after_offer(rcv_name, msg, option,
		    rcv_size);
	}
recv_logged:
	if ((is_net || is_stor || is_browser) && is_watch_rcv && recv_n < 4000) {
		recv_n++;
		ret0 = __builtin_return_address(0);
		fprintf(stderr,
		    "channelmac_nested_timeout0_v1 watcher recv pid=%d port=%u kr=0x%x msgid=0x%x ipcz=%u size=%u ret0=%p nest=%d net=%d stor=%d browser=%d drain=%d\n",
		    (int)getpid(), (unsigned)rcv_name, (unsigned)mr,
		    (mr == MACH_MSG_SUCCESS && msg != NULL) ?
			(unsigned)msg->msgh_id : 0,
		    (mr == MACH_MSG_SUCCESS && msg != NULL) ?
			machport_ipcz_msgid(msg) : 0,
		    (mr == MACH_MSG_SUCCESS && msg != NULL) ?
			(unsigned)msg->msgh_size : 0,
		    ret0, machport_in_dispatch, is_net, is_stor, is_browser,
		    drained);
		fflush(stderr);
		if (mr == MACH_MSG_SUCCESS && msg != NULL) {
			unsigned imsg;

			machport_sl1424_name_note("recv", msg, rcv_name, mr);
			machport_real_ping_note("recv", msg, rcv_name, mr);
			imsg = machport_ipcz_msgid(msg);
			if (imsg == 0 || imsg == 1) {
				machport_invite_trap_dump(
				    imsg == 0 ? "connect0_recv" : "connect1_recv",
				    rcv_name);
			}
		}
		if (mr == MACH_MSG_SUCCESS && msg != NULL &&
		    msg->msgh_id == (mach_msg_id_t)MACHPORT_HS_MJHS) {
			fprintf(stderr,
			    "channelmac_bound_v1 mjhs recv pid=%d port=%u nest=%d trailer_pid=%u size=%u net=%d stor=%d browser=%d\n",
			    (int)getpid(), (unsigned)rcv_name,
			    machport_in_dispatch,
			    machport_trailer_audit_pid(msg),
			    (unsigned)msg->msgh_size, is_net, is_stor,
			    is_browser);
			fflush(stderr);
		}
		if (mr == MACH_MSG_SUCCESS && msg != NULL &&
		    msg->msgh_id == (mach_msg_id_t)0x4d4f4a4fu &&
		    (msg->msgh_bits & MACH_MSGH_BITS_COMPLEX) != 0) {
			machport_desc12_note(msg, rcv_name, ~(uint64_t)0,
			    0xffffffffu);
		}
		if (mr == MACH_MSG_SUCCESS && msg != NULL &&
		    machport_ipcz_msgid(msg) == 20) {
			uint64_t sl;
			unsigned seq;
			uint32_t iid;
			uint32_t mojo_name;
			uint32_t mojo_flags;
			unsigned nbytes;

			(void)machport_handle_order_v1;
			sl = machport_ipcz_sublink20(msg);
			seq = machport_ipcz_proto20(msg);
			nbytes = machport_ipcz_num_bytes(msg);
			iid = 0xffffffffu;
			mojo_name = 0xffffffffu;
			mojo_flags = 0xffffffffu;
			(void)machport_mojo_hdr(msg, &iid, &mojo_name,
			    &mojo_flags);
			fprintf(stderr,
			    "handle_order_v1 pid=%d port=%u sl=%llu seq=%u nb=%u iid=%u name=%u flags=%u nest=%d kr=0x%x net=%d stor=%d browser=%d size=%u trailer_pid=%u\n",
			    (int)getpid(), (unsigned)rcv_name,
			    (unsigned long long)sl, seq, nbytes, iid, mojo_name,
			    mojo_flags, machport_in_dispatch, (unsigned)mr,
			    is_net, is_stor, is_browser,
			    (unsigned)msg->msgh_size,
			    machport_trailer_audit_pid(msg));
			fflush(stderr);
			if ((msg->msgh_bits & MACH_MSGH_BITS_COMPLEX) != 0 ||
			    (sl == 2 && seq <= 4)) {
				machport_desc12_note(msg, rcv_name, sl, seq);
			}
			machport_knote_log(
			    "handle_order_v1 port=%u sl=%llu seq=%u nb=%u iid=%u name=%u flags=%u nest=%d net=%d stor=%d browser=%d",
			    (unsigned)rcv_name, (unsigned long long)sl, seq,
			    nbytes, iid, mojo_name, mojo_flags,
			    machport_in_dispatch, is_net, is_stor, is_browser);
			if ((is_stor || is_net) &&
			    machport_ipcz_msgid(msg) == 20) {
				machport_accept20_chrome_note(msg, rcv_name,
				    sl, seq);
			}
			if (iid == 0 && mojo_name == 0 &&
			    (mojo_flags & 1u) != 0 && sl == 15) {
				machport_channelmac_bound_note(rcv_name,
				    (unsigned)sl, seq, nbytes, iid, mojo_flags,
				    machport_trailer_audit_pid(msg));
			}
			{
				uint16_t cht;
				uint16_t hdrb;
				int real_ping;

				cht = machport_msg_ch_type(msg, &hdrb, NULL);
				real_ping = (cht == 2) ||
				    (iid == 0 && mojo_name == 0 &&
				    (mojo_flags & 1u) != 0);
				if ((is_stor || is_net || is_browser) &&
				    real_ping && !machport_in_dispatch) {
				void *thisp = NULL;
				void *sub = NULL;
				void *chan = NULL;
				pthread_mutex_t *held;
				int i;

				(void)machport_helper_handle_ping_v1;
				for (i = 0; i < machport_watch_n; i++) {
					if (machport_watches[i].port ==
					    rcv_name) {
						thisp = machport_watches[i].thisp;
						sub = machport_watches[i].sub;
						chan = machport_watches[i].chan;
						break;
					}
				}
				held = machport_channelmac_held_lock(chan);
				fprintf(stderr,
				    "helper_handle_ping_v1 Handle=yes pid=%d port=%u sl=%llu seq=%u nb=%u iid=%u name=%u flags=%u ch_type=%u nest=%d this=sub=%p chan=%p lock_held=%d OnMachMessageReceived=1 net=%d stor=%d browser=%d local=0x%x remote=0x%x\n",
				    (int)getpid(), (unsigned)rcv_name,
				    (unsigned long long)sl, seq, nbytes, iid,
				    mojo_name, mojo_flags, cht,
				    machport_in_dispatch, sub,
				    chan, held != NULL, is_net, is_stor,
				    is_browser,
				    (unsigned)msg->msgh_local_port,
				    (unsigned)msg->msgh_remote_port);
				fflush(stderr);
				machport_knote_log(
				    "helper_handle_ping_v1 Handle=yes port=%u sl=%llu nest=%d this=sub=%p lock_held=%d ch_type=%u",
				    (unsigned)rcv_name,
				    (unsigned long long)sl,
				    machport_in_dispatch, sub, held != NULL,
				    cht);
				machport_hping_seen = 1;
				machport_ping_wait_send = 1;
				machport_ping_wait_sends = 0;
				machport_ping_wait_port = (unsigned)rcv_name;
				(void)thisp;
				machport_helper_getrouter23_note(rcv_name, sl);
				machport_ping_gap_note(msg, rcv_name);
				} else if ((is_stor || is_net || is_browser) &&
				    real_ping && machport_in_dispatch) {
				fprintf(stderr,
				    "helper_handle_ping_v1 Handle=skip nest=1 pid=%d port=%u sl=%llu seq=%u nb=%u iid=%u name=%u flags=%u nest=0 pending Chrome_IOThread\n",
				    (int)getpid(), (unsigned)rcv_name,
				    (unsigned long long)sl, seq, nbytes, iid,
				    mojo_name, mojo_flags);
				fflush(stderr);
				}
			}
		}
		if ((is_net || is_stor || is_browser) &&
		    mr == MACH_MSG_SUCCESS && msg != NULL &&
		    machport_ipcz_msgid(msg) == 20 &&
		    machport_ipcz_proto20(msg) == 4) {
			(void)machport_nodelink_ping_v1;
			fprintf(stderr,
			    "nodelink_ping_copyout_v1 ChannelMac OnMachMessageReceived pid=%d port=%u proto=4 local=0x%x remote=0x%x size=%u nest=%d this_path=ChannelMac net=%d browser=%d\n",
			    (int)getpid(), (unsigned)rcv_name,
			    (unsigned)msg->msgh_local_port,
			    (unsigned)msg->msgh_remote_port,
			    (unsigned)msg->msgh_size, machport_in_dispatch,
			    is_net, is_browser);
			fflush(stderr);
			machport_ping_gap_note(msg, rcv_name);
		}
	}
	return mr;
}

#define DYLD_INTERPOSE(_replacement, _replacee) \
	__attribute__((used)) static struct { \
		const void *replacement; \
		const void *replacee; \
	} _interpose_##_replacee \
	    __attribute__((section("__DATA,__interpose"))) = { \
		(const void *)(unsigned long)&_replacement, \
		(const void *)(unsigned long)&_replacee \
	    }

DYLD_INTERPOSE(machport_mach_msg, mach_msg);

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
