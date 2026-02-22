use crate::shard::Worker;
use crate::transaction::Transaction;
use crossbeam::{
    channel::{Receiver, Sender, unbounded},
    epoch::Atomic,
};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const DEFAULT_THREAD_POOL_SIZE: usize = 5;

pub struct Engine {
    workers: usize,
    pool: ThreadPool,
    receivers: Vec<Receiver<Transaction>>,
    done: Arc<AtomicBool>,
}

impl Engine {
    pub fn new(workers: usize, done: Arc<AtomicBool>) -> (Self, Vec<Sender<Transaction>>) {
        let pool = ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|id| format!("worker-{}", id))
            .build()
            .expect("failed to build thread pool");

        let mut senders = vec![];
        let mut receivers = vec![];

        for _ in 0..workers {
            let (tx, rx) = unbounded::<Transaction>();
            senders.push(tx);
            receivers.push(rx);
        }

        let engine = Self {
            workers,
            pool,
            receivers,
            done,
        };
        (engine, senders)
    }

    pub fn run(&self) {
        for worker in 0..=self.workers {
            let done = self.done.clone();
            let mut receiver = self.receivers[worker].clone();
            self.pool.spawn(move || {
                let mut worker = Worker::new(worker as u16, receiver);
                worker.run(done);
            });
        }
    }
}
