//! Self-contained TCP echo server demo.
//!
//! The server accepts one connection through `TcpListener::incoming()` and uses
//! `TcpStream::into_split()` so the owned read and write halves can drive an
//! echo loop independently. A client connects in the same runtime, verifies the
//! echoed bytes, and then exits so the example terminates.

use std::io;
use std::net::SocketAddr;

use runite::io::{AsyncReadExt, AsyncWriteExt, StreamExt};
use runite::net::{TcpListener, TcpStream};
use runite::queue_future;

#[runite::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let address = listener.local_addr()?;
    println!("[server] listening on {address}");

    let server = queue_future(async move {
        let mut incoming = listener.incoming();
        let stream = incoming.next().await.expect("incoming streams never end")?;
        println!("[server] accepted echo client");

        let (mut reader, mut writer) = stream.into_split();
        let mut buffer = [0; 1024];
        loop {
            let read = reader.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            writer.write_all(&buffer[..read]).await?;
        }
        writer.close().await?;
        println!("[server] echoed client bytes and closed");
        Ok::<(), io::Error>(())
    });

    let message = b"hello from a runite tcp client\n";
    let mut client = TcpStream::connect(address).await?;
    client.write_all(message).await?;

    let mut echoed = vec![0; message.len()];
    client.read_exact(&mut echoed).await?;
    assert_eq!(echoed, message);
    println!(
        "[client] echo: {}",
        String::from_utf8_lossy(&echoed).trim_end()
    );

    drop(client);
    server.await??;
    Ok(())
}
