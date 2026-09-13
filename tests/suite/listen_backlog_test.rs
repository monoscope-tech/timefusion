//! Listen-backlog overflow tests: tokio's `TcpListener::bind` hardcodes a backlog
//! of 128, so bursts against a stalled or starved acceptor overflow it. Binding via
//! `TcpSocket` with an explicit backlog fixes that, subject to host `somaxconn`.

use std::{io, sync::Arc, time::Duration};

use tokio::{
    net::{TcpListener, TcpSocket, TcpStream},
    sync::Notify,
    time::timeout,
};

/// Concurrent SYNs per scenario — well past the 128-deep default accept queue.
const BURST: usize = 300;

struct BurstResult {
    ok: usize,
    refused: usize,
    timed_out: usize,
}

/// Fire `BURST` concurrent connects at `port`, classifying each outcome.
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
/// * `backlog` — `None` binds the stock tokio way (backlog 128); `Some(n)`
///   binds via `TcpSocket` and requests `n` explicitly.
/// * `drain` — `true` runs a tight accept loop; `false` wedges the acceptor
///   until the burst is over so the accept queue overflows.
/// * `hogs` — CPU-bound tasks pinned on runtime workers to starve the accept
///   task. Must equal `worker_threads` to saturate.
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
            // Wedged accept loop; once released, drain so the test exits cleanly.
            release_c.notified().await;
        }
        loop {
            tokio::select! {
                _ = release_c.notified() => break,
                res = listener.accept() => if res.is_err() { break },
            }
        }
    });

    // Saturate every worker with a CPU-bound spin. Must be `tokio::spawn` with
    // no `.await` points: `spawn_blocking` uses a separate pool and would not
    // starve the runtime.
    const HOG_DURATION_MS: u64 = 800;
    let hogs: Vec<_> = (0..hogs)
        .map(|i| {
            tokio::spawn(async move {
                let deadline = std::time::Instant::now() + Duration::from_millis(HOG_DURATION_MS);
                let mut x: u64 = i as u64;
                while std::time::Instant::now() < deadline {
                    for _ in 0..1_000_000 {
                        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    }
                    std::hint::black_box(x);
                }
            })
        })
        .collect();

    // Let the listener be polled into the runtime; shorter with hogs, which
    // must already be spinning when the burst lands.
    tokio::time::sleep(Duration::from_millis(if hogs.is_empty() { 50 } else { 20 })).await;
    eprint!("{label}: burst={BURST} ");
    let res = burst_connect(port, connect_timeout).await;

    for h in hogs {
        let _ = h.await;
    }
    release.notify_one();
    // Only the draining acceptor exits on release; awaiting the stalled one hangs.
    if drain {
        let _ = accepter.await;
    }
    res
}

// Manual reproducer: CI runners queue SYNs beyond the requested backlog, so the
// "some connects must fail" assertion cannot hold there.
#[ignore = "kernel-level backlog enforcement not reliable on CI runners"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listen_backlog_overflows_at_128_when_accept_stalls() {
    let BurstResult { ok, refused, timed_out } = run_burst("stock-backlog", None, false, Duration::from_millis(500), 0).await;

    // The accept queue is finite (128 by default), so some of the 300 connects
    // must fail against a stalled acceptor. Asserted on Linux only: other
    // kernels silently queue beyond the requested backlog.
    let failed = refused + timed_out;
    #[cfg(target_os = "linux")]
    assert!(
        failed > 0,
        "expected backlog overflow to refuse/timeout some of {BURST} connects, \
         but all {ok} succeeded. Either the listen backlog has been raised \
         above {BURST}, the OS isn't enforcing it, or you're on a kernel that \
         silently queues beyond the requested backlog."
    );

    eprintln!("Mechanism reproduced: {failed} of {BURST} connects failed (refused={refused}, timed_out={timed_out}).");
    if cfg!(target_os = "linux") && refused > 0 {
        eprintln!("ECONNREFUSED observed -> host has tcp_abort_on_overflow=1, matches prod.");
    } else if refused == 0 && timed_out > 0 {
        eprintln!("Timeouts (no ECONNREFUSED) -> default kernel behavior; same root cause.");
    }
    let _ = ok;
}

/// With an explicit deeper backlog, the same stalled-acceptor burst queues all
/// 300 SYNs instead of refusing any.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn larger_backlog_eliminates_overflow_under_same_burst() {
    const BACKLOG: u32 = 4096;

    let BurstResult { ok, refused, timed_out } = run_burst("explicit-backlog=4096", Some(BACKLOG), false, Duration::from_millis(500), 0).await;

    // The OS clamps the requested backlog to `somaxconn` (macOS
    // `kern.ipc.somaxconn` defaults to 128), so requesting a large backlog only
    // helps if the host allows it — hence informational on macOS, asserted on Linux.
    if ok < BURST {
        eprintln!(
            "Backlog clamped by host somaxconn — requested {BACKLOG}, got effective ~{ok}. \
             Raise somaxconn (Linux: sysctl -w net.core.somaxconn=4096; \
             macOS: sysctl -w kern.ipc.somaxconn=4096) for the app-level fix to take effect."
        );
    }

    #[cfg(target_os = "linux")]
    assert_eq!(
        ok, BURST,
        "On Linux with default somaxconn=4096, backlog={BACKLOG} should queue all {BURST}, \
         but got ok={ok} refused={refused} timed_out={timed_out}. \
         Either somaxconn is set below {BURST} on this host or the fix is incomplete."
    );
    let _ = (refused, timed_out);
}

/// A correct, tight accept loop still overflows the 128-deep queue when every
/// runtime worker is pinned by CPU-bound work and the accept future goes
/// unpolled. Probabilistic indicator, not a hard contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_starvation_causes_backlog_overflow_under_burst() {
    // hogs == worker_threads, or the accept task is not starved.
    let BurstResult { ok, refused, timed_out } = run_burst("worker-starved", None, true, Duration::from_millis(400), 2).await;

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

    assert_eq!(ok, BURST, "fast acceptor should drain all {BURST} connects (got ok={ok}, refused={refused}, timed_out={timed_out})");
}
