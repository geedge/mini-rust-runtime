// luwenpeng 2022/11/01

use futures::StreamExt;
use mini_rust_runtime::executor::Executor;
use mini_rust_runtime::tcp::TcpListener;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

fn main() {
    let excutor = Executor::new();

    excutor.block_on(tcp_server);
}

async fn tcp_server() {
    let mut listener = TcpListener::bind("127.0.0.1:30000").unwrap();
    println!("[tcp_server] listen on: 127.0.0.1:30000");

    // NOTE：listener.next() 其实是调用的 poll_next()
    while let Some(ret) = listener.next().await {
        if let Ok((mut stream, addr)) = ret {
            println!("[tcp_server] accept new connection: {}", addr);

            // 此处只是注册，并不是此时执行
            let future = async move {
                let mut buf = [0; 4096];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(n) => {
                            println!("[tcp_server] stream {} read: {}", addr, n);

                            if n == 0 || stream.write_all(&buf[..n]).await.is_err() {
                                println!("[tcp_server] stream {} write: {}", addr, n);
                                return;
                            }
                        }
                        Err(e) => {
                            println!("[tcp_server] stream {} read: {}", addr, e);
                            return;
                        }
                    }
                }
            };

            // 将 future 封装成 task push 到任务队列中
            println!("[tcp_server] -> befor Executor::spawn()");
            Executor::spawn(future);
            println!("[tcp_server] -> after Executor::spawn()");
        }
    }
}
