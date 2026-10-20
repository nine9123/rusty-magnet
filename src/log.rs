//! Tiny leveled logger writing to stderr (level from `RUSTYMAGNET_LOG`).

use std::sync::atomic::{AtomicU8, Ordering};

pub const ERROR: u8 = 1;
pub const WARN: u8 = 2;
pub const INFO: u8 = 3;
pub const DEBUG: u8 = 4;

static LEVEL: AtomicU8 = AtomicU8::new(INFO);

pub fn init_from_env() {
    let lvl = match std::env::var("RUSTYMAGNET_LOG").unwrap_or_default().to_ascii_lowercase().as_str() {
        "error" => ERROR,
        "warn" => WARN,
        "debug" | "trace" => DEBUG,
        _ => INFO,
    };
    LEVEL.store(lvl, Ordering::Relaxed);
}

pub fn enabled(level: u8) -> bool {
    level <= LEVEL.load(Ordering::Relaxed)
}

pub fn write(level: u8, args: std::fmt::Arguments<'_>) {
    let tag = match level {
        ERROR => "ERROR",
        WARN => "WARN ",
        INFO => "INFO ",
        _ => "DEBUG",
    };
    eprintln!("{} {} {}", crate::http::rfc3339(crate::store::now()), tag, args);
}

#[macro_export]
macro_rules! log_at {
    ($lvl:expr, $($arg:tt)*) => {
        if $crate::log::enabled($lvl) {
            $crate::log::write($lvl, format_args!($($arg)*));
        }
    };
}
#[macro_export]
macro_rules! log_error { ($($arg:tt)*) => { $crate::log_at!($crate::log::ERROR, $($arg)*) }; }
#[macro_export]
macro_rules! log_warn { ($($arg:tt)*) => { $crate::log_at!($crate::log::WARN, $($arg)*) }; }
#[macro_export]
macro_rules! log_info { ($($arg:tt)*) => { $crate::log_at!($crate::log::INFO, $($arg)*) }; }
#[macro_export]
macro_rules! log_debug { ($($arg:tt)*) => { $crate::log_at!($crate::log::DEBUG, $($arg)*) }; }
