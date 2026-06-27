//! I/O throughput benchmarks exercising the io_uring (Linux) / kqueue (macOS)
//! backends end to end over loopback and the local filesystem.

mod common;

use common::time_on_runtime;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use runite::net::{TcpListener, TcpStream};

const TCP_PAYLOAD: usize = 16 * 1024;

/// Loopback TCP echo: a server task echoes a fixed payload that the client
/// writes and reads back, repeated `iters` times on a single connection.
fn bench_tcp_echo(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_echo");
    group.throughput(Throughput::Bytes(TCP_PAYLOAD as u64));
    group.bench_function("loopback_16k", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                let listener = TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind listener");
                let addr = listener.local_addr().expect("listener addr");

                let server = runite::spawn(async move {
                    let (mut stream, _peer) = listener.accept().await.expect("accept");
                    let mut buf = vec![0u8; TCP_PAYLOAD];
                    for _ in 0..iters {
                        let mut read = 0;
                        while read < TCP_PAYLOAD {
                            let n = stream.read(&mut buf[read..]).await.expect("server read");
                            if n == 0 {
                                return;
                            }
                            read += n;
                        }
                        stream.write_all(&buf).await.expect("server echo");
                    }
                });

                let mut client = TcpStream::connect(addr).await.expect("connect");
                let payload = vec![0x5au8; TCP_PAYLOAD];
                let mut buf = vec![0u8; TCP_PAYLOAD];
                for _ in 0..iters {
                    client.write_all(&payload).await.expect("client write");
                    client.read_exact(&mut buf).await.expect("client read");
                }
                drop(client);
                let _ = server.await;
            })
        });
    });
    group.finish();
}

/// Filesystem write+read round trip through the backend.
fn bench_fs_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("fs_roundtrip");
    group.throughput(Throughput::Bytes(64 * 1024));
    group.bench_function("write_read_64k", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                let payload = vec![0x42u8; 64 * 1024];
                let mut dir = std::env::temp_dir();
                dir.push(format!("runite-bench-{}", std::process::id()));
                for i in 0..iters {
                    let path = dir.with_extension(format!("{i}"));
                    runite::fs::write(&path, &payload).await.expect("write");
                    let back = runite::fs::read(&path).await.expect("read");
                    debug_assert_eq!(back.len(), payload.len());
                    let _ = runite::fs::remove_file(&path).await;
                }
            })
        });
    });
    group.finish();
}

criterion_group!(benches, bench_tcp_echo, bench_fs_roundtrip);
criterion_main!(benches);
