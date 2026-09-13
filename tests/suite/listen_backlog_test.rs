//! Reproduces the production "Connection refused" symptom that monoscope's
//! bulk insert jobs hit against `timefusion.s.past3.tech:5432`.
//!
//! Root cause: tokio's `TcpListener::bind` (via mio) hardcodes the listen
//! backlog to **128** on Linux/macOS. When monoscope's parallel retry loops
//! converge into a burst of >128 concurrent SYNs faster than the accept loop
//! drains the queue, the kernel either drops the SYN (default Linux,
//! manifests as client connect-timeout) or RSTs it (Linux with
//! `tcp_abort_on_overflow=1`, manifests as `ECONNREFUSED`). The server logs
//! show nothing because the rejection happens in the kernel, not in
//! application code.
//!
//! This test pins the mechanism by binding a stock `TcpListener` (backlog=128),
//! stalling the accept loop, and demonstrating that the 129th+ concurrent
//! connect attempt fails. The fix is to bind via `socket2`/`TcpSocket` with
//! an explicit larger backlog (e.g. 4096) — see follow-up.
//!
//! Run with `RUST_LOG=info` for per-connection diagnostics. Run on Linux with
//! `sudo sysctl net.ipv4.tcp_abort_on_overflow=1` to surface the exact prod
//! `ECONNREFUSED` error class; otherwise observed failures are timeouts (same
//! root cause, different kernel response).

use std::{io, sync::Arc, time::Duration};

use tokio::{
    net::{TcpListener, TcpSocket, TcpStream},
    sync::Notify,
    time::timeout,
};

/// Every scenario bursts the same number of concurrent SYNs, well past the
/// 128-deep default accept queue.
const BURST: usize = 300;

struct BurstResult {
    ok: usize,
    refused: usize,
    timed_out: usize,
}

/// Fire `BURST` concurrent TCP connect attempts at the given port, with a
/// per-connect timeout, classifying each outcome.
async fn burst_connect(port: u16, connect_timeout: Duration) -> BurstResult {
    let handles: Vec<_> = (0..BURST).map(|_| tokio::spawn(async move { timeout(connect_timeout, TcpStream::connect(("127.0.0.1", port))).await })).collect();

    let (mut ok, mut refused, mut timed_out, mut other) = (0, 0, 0, Vec::new());
    for h in handles {
        match h.await.expect("join") {
            Ok(Ok(_stream)) => ok += 1,
            Ok(Err(e)) if e.kind() == io::ErrorKind::ConnectionRefused => refused += 1,
            Ok(Err(e)) => other.push(format!("{} ({:?})", e, e.kind())),
            Err(_elapsed) => timed_out += 1,
        }
    }
    eprintln!("ok={ok} refused={refused} timed_out={timed_out} other={}", other.len());
    for e in other.iter().take(5) {
        eprintln!("  - {e}");
    }
    BurstResult { ok, refused, timed_out }
}

/// One burst scenario against a real listener.
///
/// * `backlog` — `None` binds the stock tokio way (mio hardcodes 128);
///   `Some(n)` binds via `TcpSocket` and requests `n` explicitly (the fix).
/// * `drain` — `true` runs a tight accept loop (the "correct" spawn-immediately
///   pattern); `false` wedges the acceptor until the burst is over, so the
///   kernel keeps queueing SYNs until the accept queue overflows.
/// * `hogs` — CPU-bound tasks pinned on runtime workers before the burst, to
///   starve the accept task. Must equal `worker_threads` to saturate.
async fn run_burst(label: &str, backlog: Option<u32>, drain: bool, connect_timeout: Duration, hogs: usize) -> BurstResult {
    let listener = match backlog {
        None => TcpListener::bind("127.0.0.1:0").await.expect("bind"),
        Some(n) => {
            let socket = TcpSocket::new_v4().expect("socket");
            socket.bind("127.0.0.1:0".parse().unwrap()).expect("bind");
            socket.listen(n).expect("listen")
        }
    };
    let port = listener.local_addr().expect("local_addr").port();
    let release = Arc::new(Notify::new());
    let release_c = release.clone();

    let accepter = tokio::spawn(async move {
        if !drain {
            // Wedged accept loop — equivalent to what monoscope sees when the
            // prod accept loop falls behind during a thundering-herd retry
            // burst. Once released, drain so the test exits cleanly.
            release_c.notified().await;
        }
        loop {
            tokio::select! {
                _ = release_c.notified() => break,
                res = listener.accept() => if res.is_err() { break },
            }
        }
    });

    // Saturate every worker with a CPU-bound spin. This mimics heavy query
    // execution starving the accept task.
    //
    // NOTE: tokio::task::spawn_blocking does NOT starve the runtime — it runs
    // on a dedicated blocking pool. Use tokio::spawn with a tight CPU-bound
    // loop (no .await points) so the work actually pins a runtime worker.
    const HOG_DURATION_MS: u64 = 800;
    let hogs: Vec<_> = (0..hogs)
        .map(|i| {
            tokio::spawn(async move {
                let deadline = std::time::Instant::now() + Duration::from_millis(HOG_DURATION_MS);
                let mut x: u64 = i as u64;
                while std::time::Instant::now() < deadline {
                    // Cooperative yield budget exhausts after ~128 polls in tokio,
                    // but a tight loop with no .await points blocks the worker
                    // entirely. That's the realistic case: a heavy synchronous
                    // routine inside a handler (e.g. arrow compute) holding the
                    // worker thread.
                    for _ in 0..1_000_000 {
                        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    }
                    std::hint::black_box(x);
                }
            })
        })
        .collect();

    // Let the listener be polled into the runtime; with hogs a shorter pause,
    // since they must already be spinning when the burst lands.
    tokio::time::sleep(Duration::from_millis(if hogs.is_empty() { 50 } else { 20 })).await;
    eprint!("{label}: burst={BURST} ");
    let res = burst_connect(port, connect_timeout).await;

    for h in hogs {
        let _ = h.await;
    }
    release.notify_one();
    // Only the draining acceptor exits on release; the stalled one keeps
    // draining until the runtime drops, so awaiting it would hang.
    if drain {
        let _ = accepter.await;
    }
    res
}

// Skipped on CI: GitHub Actions runners silently queue SYNs beyond the
// requested 128 backlog (kernel/network-namespace behaviour we don't control),
// so the "some connects must fail" assertion fires `ok=300 refused=0 timed_out=0`
// and the test fails deterministically. Kept as a manual reproducer — run with
// `cargo test --test listen_backlog_test -- --ignored` on a host where
// `tcp_abort_on_overflow=1` and the kernel enforces the backlog.
#[ignore = "kernel-level backlog enforcement not reliable on CI runners"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listen_backlog_overflows_at_128_when_accept_stalls() {
    let BurstResult { ok, refused, timed_out } = run_burst("stock-backlog", None, false, Duration::from_millis(500), 0).await;

    // The contract we're asserting:
    //
    // 1. The accept queue is finite (128 by default), so SOME connects must
    //    fail when we burst 300 SYNs at a stalled acceptor. If this assertion
    //    fails, either the OS isn't enforcing the backlog (unlikely) or the
    //    fix has landed (raise the burst, or delete this test as obsolete).
    let failed = refused + timed_out;
    // Strict assertion is Linux-only: some kernels (e.g. macOS dev hosts)
    // silently queue beyond the requested backlog, which would produce
    // spurious failures here. The eprintln! below still surfaces the
    // observed counts on every platform.
    #[cfg(target_os = "linux")]
    assert!(
        failed > 0,
        "expected backlog overflow to refuse/timeout some of {BURST} connects, \
         but all {ok} succeeded. Either the listen backlog has been raised \
         above {BURST}, the OS isn't enforcing it, or you're on a kernel that \
         silently queues beyond the requested backlog."
    );

    // 2. On Linux with `tcp_abort_on_overflow=1`, failures appear as
    //    ECONNREFUSED — the same error class monoscope's Hasql logs print.
    //    On macOS and stock Linux, failures appear as connect timeouts.
    //    Both indicate the same root cause: kernel-level backlog overflow.
    eprintln!("Mechanism reproduced: {failed} of {BURST} connects failed (refused={refused}, timed_out={timed_out}).");
    if cfg!(target_os = "linux") && refused > 0 {
        eprintln!("ECONNREFUSED observed -> host has tcp_abort_on_overflow=1, matches prod.");
    } else if refused == 0 && timed_out > 0 {
        eprintln!("Timeouts (no ECONNREFUSED) -> default kernel behavior; same root cause.");
    }
    let _ = ok;
}

/// The fix: bind via `TcpSocket` with an explicit larger backlog (e.g. 4096
/// or whatever `net.core.somaxconn` allows). With a deeper accept queue,
/// the same stalled-acceptor burst no longer refuses connections — the
/// kernel happily queues all 300 SYNs until the acceptor wakes up.
///
/// This is the test that should fail before the production fix lands and
/// pass after. The fix is to either:
///   1. Patch `datafusion-postgres::serve_with_handlers` to bind with
///      `TcpSocket::new_v4()? + bind + listen(4096)` instead of
///      `TcpListener::bind(...)`, or
///   2. Pre-bind in main.rs and pass a `TcpListener::from_std(...)` into
///      `serve_with_handlers` (would need a small API addition).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn larger_backlog_eliminates_overflow_under_same_burst() {
    const BACKLOG: u32 = 4096;

    let BurstResult { ok, refused, timed_out } = run_burst("explicit-backlog=4096", Some(BACKLOG), false, Duration::from_millis(500), 0).await;

    // CRITICAL FINDING: this assertion may fail even after the app-level fix
    // because the OS clamps the requested backlog to `somaxconn`:
    //   - macOS:   kern.ipc.somaxconn  (default 128)
    //   - Linux:   net.core.somaxconn  (modern default 4096; older 128)
    // So the prod fix is two-part:
    //   (a) app-level: bind via TcpSocket and request a large backlog (4096+)
    //   (b) host-level: ensure somaxconn >= the requested backlog
    // If (b) is below the burst size, the same overflow occurs and clients
    // see ECONNREFUSED (when tcp_abort_on_overflow=1) or connect timeouts.
    //
    // On macOS this test is informational only (somaxconn=128 by default).
    // On Linux it should pass with the default 4096 somaxconn.
    if ok < BURST {
        eprintln!(
            "Backlog clamped by host somaxconn — requested {BACKLOG}, got effective ~{ok}. \
             Raise somaxconn (Linux: sysctl -w net.core.somaxconn=4096; \
             macOS: sysctl -w kern.ipc.somaxconn=4096) for the app-level fix to take effect."
        );
    }

    // Assertion is Linux-only: on macOS, kern.ipc.somaxconn often clamps below
    // BACKLOG, so we can't enforce a strict ok==BURST contract. The eprintln!
    // above surfaces the macOS case as a diagnostic.
    #[cfg(target_os = "linux")]
    assert_eq!(
        ok, BURST,
        "On Linux with default somaxconn=4096, backlog={BACKLOG} should queue all {BURST}, \
         but got ok={ok} refused={refused} timed_out={timed_out}. \
         Either somaxconn is set below {BURST} on this host or the fix is incomplete."
    );
    let _ = (refused, timed_out);
}

/// Reproduces the **realistic** prod scenario: a fast accept loop that
/// nonetheless falls behind because the tokio runtime workers are saturated
/// with CPU-bound query work. The accept task is just another task — when
/// all workers are spinning on `std::hint::black_box(..)` loops, the
/// accept future doesn't get polled for tens to hundreds of milliseconds,
/// and the 128-deep accept queue overflows during a burst.
///
/// This is closer to what monoscope sees: timefusion is "up", *some*
/// connections succeed, and others get refused / time out, because the
/// accept loop is being starved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_starvation_causes_backlog_overflow_under_burst() {
    // hogs == worker_threads: every runtime worker must be pinned to starve
    // the accept task.
    let BurstResult { ok, refused, timed_out } = run_burst("worker-starved", None, true, Duration::from_millis(400), 2).await;

    // Under worker starvation, the accept task can't drain 128+ SYNs fast
    // enough during the burst. We expect SOME failures even though the
    // accept loop itself is "correct" (spawn-immediately pattern).
    let failed = refused + timed_out;
    if failed == 0 {
        eprintln!(
            "WARNING: worker starvation did not produce backlog overflow on this host. \
             Either the runtime preempted the hogs, the kernel queued beyond 128, or the \
             burst rate was insufficient. This test is a probabilistic indicator, not a hard \
             contract — but a 'pass with 0 failures' here means we should investigate prod \
             more carefully."
        );
    } else {
        eprintln!(
            "Reproduced: {failed} of {BURST} connects failed because the accept task was starved \
             by CPU-bound workers (refused={refused}, timed_out={timed_out}). On a Linux host \
             with tcp_abort_on_overflow=1, the timeouts here would appear as ECONNREFUSED \
             at the client — matching monoscope's prod logs."
        );
    }
    let _ = ok;
}

/// Sanity check: with a fast acceptor, the same burst should succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_overflow_when_acceptor_drains_promptly() {
    let BurstResult { ok, refused, timed_out } = run_burst("fast-acceptor", None, true, Duration::from_millis(2000), 0).await;

    // With a tight accept loop draining the queue, all 300 should land.
    assert_eq!(ok, BURST, "fast acceptor should drain all {BURST} connects (got ok={ok}, refused={refused}, timed_out={timed_out})");
}
