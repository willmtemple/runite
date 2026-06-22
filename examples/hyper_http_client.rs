use std::io::{Read as _, Write as _};
use std::net::TcpListener as StdTcpListener;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use runite::time::sleep;
use runite::{interval, queue_future};

fn spawn_demo_server() -> std::io::Result<(std::net::SocketAddr, thread::JoinHandle<()>)> {
    let listener = StdTcpListener::bind(("127.0.0.1", 0))?;
    let address = listener.local_addr()?;
    let handle = thread::Builder::new()
        .name("hyper-demo-server".into())
        .spawn(move || {
            let (mut stream, peer) = listener.accept().expect("demo server should accept");
            let mut request = [0; 1024];
            let read = stream.read(&mut request).expect("demo server should read");
            println!("[server] accepted {peer}, saw {} request bytes", read);

            let response = concat!(
                "HTTP/1.1 200 OK\r\n",
                "content-type: text/plain; charset=utf-8\r\n",
                "content-length: 24\r\n",
                "connection: close\r\n",
                "\r\n",
                "hello from runite!"
            );
            stream
                .write_all(response.as_bytes())
                .expect("demo server should reply");
        })
        .map_err(std::io::Error::other)?;
    Ok((address, handle))
}

#[runite::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (address, server) = spawn_demo_server()?;

    let stream = runite::net::TcpStream::connect(address).await?;
    let (mut sender, connection) = hyper::client::conn::http1::handshake(stream).await?;
    queue_future(async move {
        if let Err(error) = connection.await {
            eprintln!("[runtime] hyper connection ended with error: {error}");
        }
    });

    println!("Sleeping a moment to let the server start...");
    let ticker = interval(Duration::from_millis(400), || println!("..."));
    sleep(Duration::from_secs(2)).await;
    ticker.cancel();
    println!("Let's go!");

    let request = Request::builder()
        .method("GET")
        .uri(format!("http://{address}/demo"))
        .header("host", address.to_string())
        .body(Empty::<Bytes>::new())?;
    let response = sender.send_request(request).await?;
    let status = response.status();
    let body = response.into_body().collect().await?.to_bytes();

    println!(
        "[client] status={status}, body={}",
        String::from_utf8_lossy(&body)
    );

    server
        .join()
        .expect("demo server thread should exit cleanly");
    Ok(())
}
