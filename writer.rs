use std::sync::atomic;

struct Writer {
    end: std::sync::atomic::AtomicBool,
}

impl Writer {
    fn new() -> Self {
        Writer {
            end: atomic::AtomicBool::new(true),
        }
    }
}
