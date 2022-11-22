use std::{cell::Cell, future, path::Path, pin::Pin, rc::Rc};

use tokio::io;

use esthri::opts;

// From https://github.com/tokio-rs/tokio/blob/master/tokio/tests/async_send_sync.rs

// The names of these structs behaves better when sorted.
// Send: Yes, Sync: Yes
#[derive(Clone)]
struct YY {}

// Send: Yes, Sync: No
#[derive(Clone)]
struct YN {
    _value: Cell<u8>,
}

// Send: No, Sync: No
#[derive(Clone)]
struct NN {
    _value: Rc<u8>,
}

#[allow(dead_code)]
type BoxFutureSync<T> = Pin<Box<dyn future::Future<Output = T> + Send + Sync>>;
#[allow(dead_code)]
type BoxFutureSend<T> = Pin<Box<dyn future::Future<Output = T> + Send>>;
#[allow(dead_code)]
type BoxFuture<T> = Pin<Box<dyn future::Future<Output = T>>>;

#[allow(dead_code)]
type BoxAsyncRead = Pin<Box<dyn io::AsyncBufRead + Send + Sync>>;
#[allow(dead_code)]
type BoxAsyncSeek = Pin<Box<dyn io::AsyncSeek + Send + Sync>>;
#[allow(dead_code)]
type BoxAsyncWrite = Pin<Box<dyn io::AsyncWrite + Send + Sync>>;

#[allow(dead_code)]
fn require_send<T: Send>(_t: &T) {}
#[allow(dead_code)]
fn require_sync<T: Sync>(_t: &T) {}
#[allow(dead_code)]
fn require_unpin<T: Unpin>(_t: &T) {}

#[allow(dead_code)]
struct Invalid;

trait AmbiguousIfSend<A> {
    fn some_item(&self) {}
}
impl<T: ?Sized> AmbiguousIfSend<()> for T {}
impl<T: ?Sized + Send> AmbiguousIfSend<Invalid> for T {}

trait AmbiguousIfSync<A> {
    fn some_item(&self) {}
}
impl<T: ?Sized> AmbiguousIfSync<()> for T {}
impl<T: ?Sized + Sync> AmbiguousIfSync<Invalid> for T {}

trait AmbiguousIfUnpin<A> {
    fn some_item(&self) {}
}
impl<T: ?Sized> AmbiguousIfUnpin<()> for T {}
impl<T: ?Sized + Unpin> AmbiguousIfUnpin<Invalid> for T {}

macro_rules! into_todo {
    ($typ:ty) => {{
        let x: $typ = todo!();
        x
    }};
}

macro_rules! async_assert_fn_send {
    (Send & $(!)?Sync & $(!)?Unpin, $value:expr) => {
        require_send(&$value);
    };
    (!Send & $(!)?Sync & $(!)?Unpin, $value:expr) => {
        AmbiguousIfSend::some_item(&$value);
    };
}

macro_rules! async_assert_fn_sync {
    ($(!)?Send & Sync & $(!)?Unpin, $value:expr) => {
        require_sync(&$value);
    };
    ($(!)?Send & !Sync & $(!)?Unpin, $value:expr) => {
        AmbiguousIfSync::some_item(&$value);
    };
}

macro_rules! async_assert_fn_unpin {
    ($(!)?Send & $(!)?Sync & Unpin, $value:expr) => {
        require_unpin(&$value);
    };
    ($(!)?Send & $(!)?Sync & !Unpin, $value:expr) => {
        AmbiguousIfUnpin::some_item(&$value);
    };
}

macro_rules! async_assert_fn {
    ($($f:ident $(< $($generic:ty),* > )? )::+($($arg:ty),*): $($tok:tt)*) => {
        #[allow(unreachable_code)]
        #[allow(unused_variables)]
        const _: fn() = || {
            let f = $($f $(::<$($generic),*>)? )::+( $( into_todo!($arg) ),* );
            async_assert_fn_send!($($tok)*, f);
            async_assert_fn_sync!($($tok)*, f);
            async_assert_fn_unpin!($($tok)*, f);
        };
    };
}

async_assert_fn!(
    esthri::upload(_, &str, &str, &Path, opts::EsthriPutOptParams): Send & !Sync & !Unpin
);
