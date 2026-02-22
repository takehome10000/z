use crate::account::Account;
use crate::transaction::Transaction;
use crossbeam::channel::{Receiver, Sender};
use heapless::index_map::FnvIndexMap;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};

const ACCOUNTS_PER_SHARD: usize = 500_000;

pub struct Worker {
    pub id: u16,
    pub txs: Receiver<Transaction>,
}

impl Worker {
    pub fn new(id: u16, txs: Receiver<Transaction>) -> Self {
        Self { id, txs }
    }

    pub fn run(&mut self, done: Arc<AtomicBool>) {
        let mut shard = Shard::new();
        loop {
            if done.load(Relaxed) {
                break;
            }
            match self.txs.recv() {
                Ok(transaction) => match transaction {
                    Transaction::Deposit(tx) => {
                        let client_id = tx.client as u16;
                        if let Some(account) = shard.accounts.get_mut(&client_id) {
                            account.borrow_mut().deposit(tx);
                        } else {
                            let acc = Rc::new(RefCell::new(Account::new(
                                client_id,
                                Transaction::Deposit(tx),
                            )));
                            shard.accounts.insert(client_id, acc);
                        }
                    }
                    Transaction::Withdrawal(tx) => {
                        let client_id = tx.client as u16;
                        if let Some(account) = shard.accounts.get_mut(&client_id) {
                            account.borrow_mut().withdraw(tx);
                        } else {
                            let acc = Rc::new(RefCell::new(Account::new(
                                client_id,
                                Transaction::Withdrawal(tx),
                            )));
                            shard.accounts.insert(client_id, acc);
                        }
                    }
                    Transaction::Dispute(tx) => {
                        let client_id = tx.client as u16;
                        if let Some(account) = shard.accounts.get_mut(&client_id) {
                            account.borrow_mut().dispute(tx);
                        }
                    }
                    Transaction::Resolve(tx) => {
                        let client_id = tx.client as u16;
                        if let Some(account) = shard.accounts.get_mut(&client_id) {
                            account.borrow_mut().resolve(tx);
                        }
                    }
                    Transaction::Chargeback(tx) => {
                        let client_id = tx.client as u16;
                        if let Some(account) = shard.accounts.get_mut(&client_id) {
                            account.borrow_mut().chargeback(tx);
                        }
                    }
                },
                Err(_) => {
                    // channel disconnected — shut down cleanly
                    eprintln!("worker {} shutting down", self.id);
                    self.finalize();
                    return;
                }
            }
        }
        // todo: complete shard here
    }

    fn finalize(&mut self) {}
}

struct Shard {
    accounts: FnvIndexMap<u16, Rc<RefCell<Account>>, ACCOUNTS_PER_SHARD>,
}

impl Shard {
    fn new() -> Self {
        Shard {
            accounts: FnvIndexMap::new(),
        }
    }
}
