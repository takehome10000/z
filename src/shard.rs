use crate::account::Account;
use crate::output::AccountOutput;
use crate::transaction::{PendingWithdraw, Transaction};
use crossbeam::channel::Receiver;
use heapless::Deque;
use indexmap::IndexMap;
use smallvec::SmallVec;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Acquire;
use std::time::{SystemTime, UNIX_EPOCH};

const DISPUTE_WINDOW_MILLISECONDS: u128 = 1;
const PENDING_QUEUE_SIZE: usize = 256;

pub struct Worker {
    pub id: u16,
    pub txs: Receiver<Vec<Transaction>>,
}

impl Worker {
    pub fn new(id: u16, txs: Receiver<Vec<Transaction>>) -> Self {
        Self { id, txs }
    }

    pub fn run(&mut self, done: Arc<AtomicBool>) -> Vec<AccountOutput> {
        let mut account_shard = AccountShard::new();

        loop {
            if done.load(Acquire) {
                dbg!("received trigger!");
                break;
            }

            if let Some(ready) = account_shard.ready_withdrawals() {
                for pw in ready {
                    if let Some(account) = account_shard.accounts.get_mut(&pw.tx.client) {
                        account.borrow_mut().withdraw(pw.tx);
                    }
                }
            }

            match self.txs.try_recv() {
                Ok(transaction) => match transaction[0] {
                    Transaction::Deposit(tx) => {
                        if let Some(account) = account_shard.accounts.get_mut(&tx.client) {
                            account.borrow_mut().deposit(tx);
                        } else {
                            let acc = Rc::new(RefCell::new(Account::new(tx.client)));
                            acc.borrow_mut().deposit(tx);
                            account_shard.accounts.insert(tx.client, acc);
                        }
                    }
                    Transaction::PendingWithdrawal(tx) => {
                        let Some(arrival_time) = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map_err(|clock_error| {
                                dbg!("system clock failed {:?}", clock_error);
                            })
                            .ok()
                            .map(|d| d.as_millis())
                        else {
                            continue;
                        };
                        let pw = PendingWithdraw { arrival_time, tx };
                        account_shard.pending_withdraws.push_back(pw).ok();
                    }
                    Transaction::Dispute(tx) => {
                        if let Some(account) = account_shard.accounts.get_mut(&tx.client) {
                            account.borrow_mut().dispute(tx);
                        }
                    }
                    Transaction::Resolve(tx) => {
                        if let Some(account) = account_shard.accounts.get_mut(&tx.client) {
                            account.borrow_mut().resolve(tx);
                        }
                    }
                    Transaction::Chargeback(tx) => {
                        if let Some(account) = account_shard.accounts.get_mut(&tx.client) {
                            account.borrow_mut().chargeback(tx);
                        }
                    }
                },
                Err(_) => continue,
            }
        }

        account_shard
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

struct AccountShard {
    pending_withdraws: Deque<PendingWithdraw, PENDING_QUEUE_SIZE>,
    accounts: IndexMap<u16, Rc<RefCell<Account>>>,
}

impl AccountShard {
    fn new() -> Self {
        AccountShard {
            pending_withdraws: Deque::new(),
            accounts: IndexMap::new(),
        }
    }

    fn ready_withdrawals(&mut self) -> Option<SmallVec<[PendingWithdraw; 10]>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|clock_error| {
                dbg!("system clock failed {:?}", clock_error);
            })
            .ok()?
            .as_millis();

        let mut ready: SmallVec<[PendingWithdraw; 10]> = SmallVec::new();
        while ready.len() < 10 {
            match self.pending_withdraws.front() {
                Some(pw) if now >= pw.arrival_time + DISPUTE_WINDOW_MILLISECONDS => {
                    if let Some(pw) = self.pending_withdraws.pop_front() {
                        ready.push(pw);
                    }
                }
                _ => break,
            }
        }

        Some(ready)
    }
}
