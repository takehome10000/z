use crate::output::AccountOutput;
use crate::shard::Worker;
use crate::transaction::Transaction;
use crossbeam::channel::{Receiver, Sender, unbounded};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;

pub struct Engine {
    receivers: Vec<Receiver<Vec<Transaction>>>,
    done: Arc<AtomicBool>,
}

impl Engine {
    pub fn new(
        workers: usize,
        done: Arc<AtomicBool>,
    ) -> anyhow::Result<(Self, Vec<Sender<Vec<Transaction>>>)> {
        let mut senders = vec![];
        let mut receivers = vec![];
        for _ in 0..=workers {
            let (tx, rx) = unbounded::<Vec<Transaction>>();
            senders.push(tx);
            receivers.push(rx);
        }
        let engine = Self { receivers, done };
        Ok((engine, senders))
    }

    pub fn run(self) -> anyhow::Result<Vec<AccountOutput>> {
        let (tx, rx) = crossbeam::channel::unbounded::<AccountOutput>();
        let mut handles = vec![];

        for (id, receiver) in self.receivers.into_iter().enumerate() {
            let done = self.done.clone();
            let tx = tx.clone();
            let handle = thread::Builder::new().name(id.to_string()).spawn(move || {
                core_affinity::set_for_current(core_affinity::CoreId { id });
                let mut worker = Worker::new(id as u16, receiver);
                let output_accounts = worker.run(done);
                for account in output_accounts {
                    tx.send(account).ok();
                }
            })?;
            handles.push(handle);
        }
        drop(tx);
        for handle in handles {
            handle.join().ok();
        }
        let mut results = vec![];
        for account in rx {
            results.push(account);
        }
        Ok(results)
    }
}
