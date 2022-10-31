// luwenpeng 2022/11/01

use crate::reactor::Reactor;
use futures::future::LocalBoxFuture;
use futures::Future;
use futures::FutureExt;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;

/******************************************************************************
 * thread_local 的静态变量 THREAD_LOCAL_EXECUTOR
 ******************************************************************************/

scoped_tls::scoped_thread_local!(pub(crate) static THREAD_LOCAL_EXECUTOR: Executor);

/******************************************************************************
 * Task
 ******************************************************************************/

pub struct Task {
    future: RefCell<LocalBoxFuture<'static, ()>>,
}

impl Task {
    fn enqueue(self: Rc<Self>) {
        Self::enqueue_by_ref(&self)
    }

    fn enqueue_by_ref(self: &Rc<Self>) {
        THREAD_LOCAL_EXECUTOR.with(|executor| executor.local_queue.push(self.clone()));
    }
}

/******************************************************************************
 * TaskQueue
 ******************************************************************************/

pub struct TaskQueue {
    queue: RefCell<VecDeque<Rc<Task>>>,
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskQueue {
    pub fn new() -> Self {
        const DEFAULT_TASK_QUEUE_SIZE: usize = 4096;
        Self::new_with_capacity(DEFAULT_TASK_QUEUE_SIZE)
    }

    pub fn new_with_capacity(capacity: usize) -> Self {
        Self {
            queue: RefCell::new(VecDeque::with_capacity(capacity)),
        }
    }

    pub(crate) fn push(&self, runnable: Rc<Task>) {
        println!("[task_queue] push task");

        self.queue.borrow_mut().push_back(runnable);
    }

    pub(crate) fn pop(&self) -> Option<Rc<Task>> {
        println!("[task_queue] pop task");

        self.queue.borrow_mut().pop_front()
    }
}

/******************************************************************************
 * Executor
 ******************************************************************************/

pub struct Executor {
    local_queue: TaskQueue,                   // 任务队列
    pub(crate) reactor: Rc<RefCell<Reactor>>, // Reactor

    // Make sure the type is `!Send` and `!Sync`.
    marker: PhantomData<Rc<()>>,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    pub fn new() -> Self {
        Self {
            local_queue: TaskQueue::default(),
            reactor: Rc::new(RefCell::new(Reactor::default())),
            marker: PhantomData,
        }
    }

    pub fn spawn(future: impl Future<Output = ()> + 'static) {
        println!("[executor] spawn, wrap future to task, and push task to queue");

        let task = Rc::new(Task {
            future: RefCell::new(future.boxed_local()),
        });
        THREAD_LOCAL_EXECUTOR.with(|executor| executor.local_queue.push(task));
    }

    pub fn block_on<F, T, O>(&self, f: F) -> O
    where
        F: Fn() -> T,
        T: Future<Output = O> + 'static,
    {
        println!("[executor] block_on");

        let _waker = waker_fn::waker_fn(|| {});
        let ctx = &mut Context::from_waker(&_waker);

        THREAD_LOCAL_EXECUTOR.set(self, || {
            // 此处的 future 为 async fn tcp_server()
            let future = f();
            pin_utils::pin_mut!(future);

            loop {
                println!("[executor] -> loop");

                // return if the outer future is ready
                if let std::task::Poll::Ready(task) = future.as_mut().poll(ctx) {
                    println!("[executor] future poll(1), return ready");
                    break task;
                }

                println!("[executor] consume all tasks");

                // consume all tasks
                while let Some(task) = self.local_queue.pop() {
                    println!("[executor] pop task frome queue");

                    let future = task.future.borrow_mut();

                    let waker = wrap_task_to_waker(task.clone()); // 此处将 task  包装成 Waker
                    let mut context = Context::from_waker(&waker); // 此处将 Waker 包装成 Context

                    let _ = Pin::new(future).as_mut().poll(&mut context);
                    println!("[executor] future poll(2), return");

                    // 此处默认执行：[RawWaker] drop_waker()
                }

                // no task to execute now, it may ready
                if let std::task::Poll::Ready(task) = future.as_mut().poll(ctx) {
                    println!("[executor] future poll(3), return ready");
                    break task;
                }

                // block for io
                println!("[executor] reactor->wait()");
                self.reactor.borrow_mut().wait();
            }
        })
    }
}

/******************************************************************************
 * waker
 ******************************************************************************/

/*
pub struct Context<'a> {
    waker: &'a Waker,
    _marker: PhantomData<fn(&'a ()) -> &'a ()>,
}

pub struct Waker {
    waker: RawWaker,
}

pub struct RawWaker {
    data: *const (),
    vtable: &'static RawWakerVTable,
}
*/

fn wrap_task_to_waker(task: Rc<Task>) -> Waker {
    println!("[executor] ->wrap_task_to_waker(), wrap task to Waker");

    let ptr = Rc::into_raw(task) as *const ();
    let vtable = &Helper::VTABLE; // VTABLE 为 const 定义的 RawWakerVTable
    unsafe { Waker::from_raw(RawWaker::new(ptr, vtable)) }
}

/******************************************************************************
 * Helper
 ******************************************************************************/

struct Helper;

impl Helper {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_waker,
        Self::wake,
        Self::wake_by_ref,
        Self::drop_waker,
    );

    // 将 task 封装成 RawWaker
    unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
        println!("[RawWaker] ->clone_waker(), wrap task to RawWaker");

        increase_refcount(ptr);
        let vtable = &Self::VTABLE;
        RawWaker::new(ptr, vtable)
    }

    //  将 task 添加到任务队列中
    unsafe fn wake(ptr: *const ()) {
        println!("[RawWaker] ->wake(), add task to queue");

        let task = Rc::from_raw(ptr as *const Task);
        task.enqueue();
    }

    // 将 task 添加到任务队列中
    unsafe fn wake_by_ref(ptr: *const ()) {
        println!("[RawWaker] wake_by_ref(), add task to queue");

        let task = mem::ManuallyDrop::new(Rc::from_raw(ptr as *const Task));
        task.enqueue_by_ref();
    }

    unsafe fn drop_waker(ptr: *const ()) {
        println!("[RawWaker] ->drop_waker(), drop RawWaker");

        drop(Rc::from_raw(ptr as *const Task));
    }
}

#[allow(clippy::redundant_clone)] // The clone here isn't actually redundant.
unsafe fn increase_refcount(data: *const ()) {
    // Retain Rc, but don't touch refcount by wrapping in ManuallyDrop
    let rc = mem::ManuallyDrop::new(Rc::<Task>::from_raw(data as *const Task));
    // Now increase refcount, but don't drop new refcount either
    let _rc_clone: mem::ManuallyDrop<_> = rc.clone();
}
