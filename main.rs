use std::env;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

mod account;
mod engine;
mod io;
mod output;
mod shard;
mod transaction;

use engine::Engine;
use io::ConcurrentAsyncFileDescriptorReader;
use output::output_accounts;


use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};

fn main() {
    let args: Vec<String> = env::args().collect();
    let filename = args.get(1).expect("Usage: program <filename>").clone();
    let mut done = Arc::new(AtomicBool::new(false));
    let (engine, senders) = Engine::new(5, done.clone());

    std::thread::spawn(move || engine.run());

    let mut reader_io = ConcurrentAsyncFileDescriptorReader::new(senders);
    let mut output = 

    let mut files = vec![filename.clone()];

    reader_io.consume(files);
    done.store(true, SeqCst);
    std::thread::sleep(std::time::Duration::from_micros(500));
    output_accounts(shards)
}
