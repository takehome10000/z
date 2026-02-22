use crate::account::Account;
use crate::output::AccountOutput;
use crate::transaction::Transaction;
use crossbeam::channel::Receiver;
use indexmap::IndexMap;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Acquire;

pub struct Worker {
    pub id: u16,
    pub txs: Receiver<Vec<Transaction>>,
}

impl Worker {
    pub fn new(id: u16, txs: Receiver<Vec<Transaction>>) -> Self {
        Self { id, txs }
    }

    pub fn run(&mut self, done: Arc<AtomicBool>) -> Vec<AccountOutput> {
        let mut shard = Shard::new();
        loop {
            std::thread::sleep(std::time::Duration::from_micros(1000));
            if done.load(Acquire) {
                dbg!("received trigger!");
                break;
            }
            match self.txs.try_recv() {
                Ok(transaction) => match transaction[0] {
                    Transaction::Deposit(tx) => {
                        dbg!("depossit incoming tx client {} id {}!", tx.client, tx.id);
                        let client_id = tx.client as u16;
                        if let Some(account) = shard.accounts.get_mut(&client_id) {
                            account.borrow_mut().deposit(tx);
                        } else {
                            let acc = Rc::new(RefCell::new(Account::new(
                                client_id,
                                Transaction::Deposit(tx),
                            )));
                            acc.borrow_mut().deposit(tx);
                            shard.accounts.insert(client_id, acc);
                        }
                    }
                    Transaction::Withdrawal(tx) => {
                        dbg!("withdrawing");
                        if let Some(account) = shard.accounts.get_mut(&tx.client) {
                            account.borrow_mut().withdraw(tx);
                        }
                    }
                    Transaction::Dispute(tx) => {
                        dbg!("incoming tx client {:?} id {:?}!");
                        if let Some(account) = shard.accounts.get_mut(&tx.client) {
                            account.borrow_mut().dispute(tx);
                        }
                    }
                    Transaction::Resolve(tx) => {
                        dbg!("incoming tx client {:?} id {:?}!");
                        if let Some(account) = shard.accounts.get_mut(&tx.client) {
                            account.borrow_mut().resolve(tx);
                        }
                    }
                    Transaction::Chargeback(tx) => {
                        dbg!("incoming tx client {:?} id {:?}!");
                        let client_id = tx.client as u16;
                        if let Some(account) = shard.accounts.get_mut(&client_id) {
                            account.borrow_mut().chargeback(tx);
                        }
                    }
                },
                Err(_) => continue,
            }
        }
        shard
            .accounts
            .iter()
            .map(|(_, account)| {
                let account = account.borrow();
                AccountOutput {
                    client: account.client(),
                    available: account.book().available_funds,
                    held: account.book().held_funds,
                    total: account.book().total_funds,
                    locked: account.locked(),
                }
            })
            .collect::<Vec<AccountOutput>>()
    }
}

struct Shard {
    accounts: IndexMap<u16, Rc<RefCell<Account>>>,
}

impl Shard {
    fn new() -> Self {
        Shard {
            accounts: IndexMap::new(),
        }
    }
}
