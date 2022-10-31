// luwenpeng 2022/11/01

use polling::Event;
use polling::Poller;
use std::cell::RefCell;
use std::os::unix::prelude::AsRawFd;
use std::os::unix::prelude::RawFd;
use std::rc::Rc;
use std::task::Context;
use std::task::Waker;
use std::thread::sleep;

/*
 * 通过 executor 获取 reactor
 *
 * 单线程中
 * Rc<T>          : (多个所有者可读)
 * RefCell<T>     : (一个所有者可写)
 * Rc<RefCell<T>> : (多个所有者可写)
 */
#[inline]
pub(crate) fn get_reactor() -> Rc<RefCell<Reactor>> {
    crate::executor::THREAD_LOCAL_EXECUTOR.with(|executor| executor.reactor.clone())
}

/******************************************************************************
 * Reactor
 *
 * fun:
 *
 * reactor.new(); 创建 reactor
 * reactor.add(); 将 fd 添加到 reactor 的 epoll 中，但未设置读写事件
 * reactor.del(); 将 fd 从
 * reactor.mod_read();
 * reactor.mod_write();
 * reactor.wait();
 ******************************************************************************/

#[derive(Debug)]
pub struct Reactor {
    poller: Poller,                                   // poll
    waker_mapping: rustc_hash::FxHashMap<u64, Waker>, // sockfd 与 waker 的映射关系
    buffer: Vec<Event>,                               // Event 的就绪队列
}

impl Reactor {
    pub fn new() -> Self {
        println!("[reactor] new");

        Self {
            poller: Poller::new().unwrap(),
            waker_mapping: Default::default(),
            buffer: Vec::with_capacity(2048),
        }
    }

    pub fn add(&mut self, fd: RawFd) {
        println!("[reactor] add fd: {}, event: none", fd);

        // 将 fd 设置为 nonblocking
        let flags =
            nix::fcntl::OFlag::from_bits(nix::fcntl::fcntl(fd, nix::fcntl::F_GETFL).unwrap())
                .unwrap();
        let flags_nonblocking = flags | nix::fcntl::OFlag::O_NONBLOCK;
        nix::fcntl::fcntl(fd, nix::fcntl::F_SETFL(flags_nonblocking)).unwrap();

        // 将 fd 添加到 poller 中
        self.poller
            .add(fd, polling::Event::none(fd as usize))
            .unwrap();
    }

    pub fn del(&mut self, fd: RawFd) {
        println!("[reactor] del fd: {}, event: all", fd,);

        // 将 fd 读写事件的 ctx 从 map 中删除
        self.waker_mapping.remove(&(fd as u64 * 2));
        self.waker_mapping.remove(&(fd as u64 * 2 + 1));

        // 将 fd 从 poller 中移除
        self.poller.delete(fd).unwrap();
    }

    pub fn mod_read(&mut self, fd: RawFd, ctx: &mut Context) {
        println!("[reactor] mod fd: {}, event: read", fd,);

        // 将 fd 可读事件的 ctx 存储到 map 中
        self.waker_mapping
            .insert(fd as u64 * 2, ctx.waker().clone());

        // fd 注册可读事件
        let event = polling::Event::readable(fd as usize);
        self.poller.modify(fd, event);
    }

    pub fn mod_write(&mut self, fd: RawFd, ctx: &mut Context) {
        println!("[reactor] mod fd: {}, event: write", fd,);

        // 将 fd 可写事件的 ctx 存储到 map 中
        self.waker_mapping
            .insert(fd as u64 * 2 + 1, ctx.waker().clone());

        // fd 注册可写事件
        let event = polling::Event::writable(fd as usize);
        self.poller.modify(fd, event);
    }

    pub fn wait(&mut self) {
        println!("[reactor] waiting ...");

        // TODO 增加定时器 timeout
        self.poller.wait(&mut self.buffer, None);
        println!("[reactor] {} events is ready", self.buffer.len());

        for i in 0..self.buffer.len() {
            let event = self.buffer.swap_remove(0);

            // TODO 是否会出现 readable && writable 的 event

            // 优先处理读事件
            if event.readable {
                if let Some(waker) = self.waker_mapping.remove(&(event.key as u64 * 2)) {
                    println!(
                        "[reactor] wake fd: {}, event: read, action: run waker.wake()",
                        event.key,
                    );
                    waker.wake();
                }
            }

            if event.writable {
                if let Some(waker) = self.waker_mapping.remove(&(event.key as u64 * 2 + 1)) {
                    println!(
                        "[reactor] wake fd: {}, event: write, action: run waker.wake()",
                        event.key,
                    );
                    waker.wake();
                }
            }
        }
    }
}

impl Default for Reactor {
    fn default() -> Self {
        Self::new()
    }
}
