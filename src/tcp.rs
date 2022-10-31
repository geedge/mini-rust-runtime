// luwenpeng 2022/11/01

use crate::reactor::get_reactor;
use crate::reactor::Reactor;
use futures::Stream;
use socket2::Domain;
use socket2::Protocol;
use socket2::Socket;
use socket2::Type;
use std::cell::RefCell;
use std::io;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener as StdTcpListener;
use std::net::TcpStream as StdTcpStream;
use std::net::ToSocketAddrs;
use std::os::unix::prelude::AsRawFd;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Poll;
use std::thread::sleep;

/******************************************************************************
 * TcpListener
 ******************************************************************************/

#[derive(Debug)]
pub struct TcpListener {
    reactor: Weak<RefCell<Reactor>>,
    listener: StdTcpListener,
}

impl TcpListener {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<Self, io::Error> {
        // 设置 addr
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "empty address"))?;

        let domain = if addr.is_ipv6() {
            Domain::IPV6
        } else {
            Domain::IPV4
        };

        let addr = socket2::SockAddr::from(addr);

        // 创建 listenfd
        let listenfd = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

        // 设置 reuse addr
        listenfd.set_reuse_address(true)?;

        // bind
        listenfd.bind(&addr)?;

        // listen
        listenfd.listen(1024)?;

        // 通过 executor 获取 reactor
        let reactor = get_reactor();

        // 将 listenfd 添加到 reactor 的 epoll 中，此时未设置 readable or writable 事件
        reactor.borrow_mut().add(listenfd.as_raw_fd());

        println!("[tcp_listener] create listenfd: {}", listenfd.as_raw_fd(),);

        Ok(Self {
            reactor: Rc::downgrade(&reactor),
            listener: listenfd.into(),
        })
    }
}

impl Stream for TcpListener {
    type Item = std::io::Result<(TcpStream, SocketAddr)>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.listener.accept() {
            // listenfd 上有新的连接到来
            Ok((stream, addr)) => {
                println!(
                    "[tcp_listener] Stream->poll_next(): accept new connection: {}",
                    addr
                );

                // 将 listenfd 注册到 reactor 的 epoll 中，期待可读事情
                let reactor = self.reactor.upgrade().unwrap();
                reactor.borrow_mut().mod_read(self.listener.as_raw_fd(), cx);

                // NOTE: stream.into() 时会调用 TcpStream.from() 将 streamfd 注册到 reactor 中
                Poll::Ready(Some(Ok((stream.into(), addr))))
            }

            // listenfd 返回 wouldblock 错误
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                println!("[tcp_listener] Stream->poll_next(): accept error: would block");

                // 将 listenfd 注册到 reactor 的 epoll 中，期待可读事情
                let reactor = self.reactor.upgrade().unwrap();
                reactor.borrow_mut().mod_read(self.listener.as_raw_fd(), cx);

                Poll::Pending
            }

            // listenfd 返回其他错误
            Err(e) => {
                println!("[tcp_listener] Stream->poll_next(): accept error: {}", e);

                Poll::Ready(Some(Err(e)))
            }
        }
    }
}

/******************************************************************************
 * TcpStream
 ******************************************************************************/

#[derive(Debug)]
pub struct TcpStream {
    stream: StdTcpStream,
}

impl From<StdTcpStream> for TcpStream {
    fn from(stream: StdTcpStream) -> Self {
        println!(
            "[tcp_stream] From->from(): add streamfd {} to reactor",
            stream.as_raw_fd()
        );

        let reactor = get_reactor();
        reactor.borrow_mut().add(stream.as_raw_fd());
        Self { stream }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        println!(
            "[tcp_stream] Drop->drop(): del streamfd {} from reactor",
            self.stream.as_raw_fd()
        );

        let reactor = get_reactor();
        reactor.borrow_mut().del(self.stream.as_raw_fd());
    }
}

impl tokio::io::AsyncRead for TcpStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let fd = self.stream.as_raw_fd();

        unsafe {
            let b = &mut *(buf.unfilled_mut() as *mut [std::mem::MaybeUninit<u8>] as *mut [u8]);
            match self.stream.read(b) {
                Ok(n) => {
                    println!("[tcp_stream] AsyncRead->poll_read(): fd {}, len: {}", fd, n);

                    buf.assume_init(n);
                    buf.advance(n);
                    Poll::Ready(Ok(()))
                }

                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    println!(
                        "[tcp_stream] AsyncRead->poll_read(): fd {}, err: would block",
                        fd
                    );

                    let reactor = get_reactor();
                    reactor.borrow_mut().mod_read(self.stream.as_raw_fd(), cx);
                    Poll::Pending
                }

                Err(e) => {
                    println!("[tcp_stream] AsyncRead->poll_read(): fd {}, err: {}", fd, e);

                    Poll::Ready(Err(e))
                }
            }
        }
    }
}

impl tokio::io::AsyncWrite for TcpStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.stream.write(buf) {
            Ok(n) => {
                println!(
                    "[tcp_stream] AsyncWrite->poll_write(): fd {}, len: {}",
                    self.stream.as_raw_fd(),
                    n
                );

                Poll::Ready(Ok(n))
            }

            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                println!(
                    "[tcp_stream] AsyncWrite->poll_write(): fd {}, err: would block",
                    self.stream.as_raw_fd()
                );

                let reactor = get_reactor();
                reactor.borrow_mut().mod_write(self.stream.as_raw_fd(), cx);
                Poll::Pending
            }

            Err(e) => {
                println!(
                    "[tcp_stream] AsyncWrite->poll_write(): fd {}, err: {}",
                    self.stream.as_raw_fd(),
                    e
                );

                Poll::Ready(Err(e))
            }
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        println!(
            "[tcp_stream] AsyncWrite->poll_flush(): fd {}",
            self.stream.as_raw_fd()
        );

        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        println!(
            "[tcp_stream] AsyncWrite->poll_shutdown(): fd {}",
            self.stream.as_raw_fd()
        );

        self.stream.shutdown(std::net::Shutdown::Write)?;
        Poll::Ready(Ok(()))
    }
}
