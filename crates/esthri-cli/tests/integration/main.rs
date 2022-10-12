#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

mod cli_test;
#[cfg(not(windows))]
mod http_server;
