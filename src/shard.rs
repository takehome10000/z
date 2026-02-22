use crate::account::Account;
use crate::output::AccountOutput;
use crate::transaction::Transaction;
use crossbeam::channel::Receiver;
use heapless::Deque;

use indexmap::IndexMap;
use smallvec::{SmallVec, smallvec};
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
        let mut account_shard = AccountShard::new();
        loop {
            if done.load(Acquire) {
                dbg!("received trigger!");
                break;
            }

            // get all the accounts that may pending withdraw
            // transactions that may have past its dispute window
            if let Some(client_accounts) = account_shard.accounts_with_pending_withdraws() {
                for client_account in client_accounts {
                    if let Some(acc) = account_shard.accounts.get_mut(&client_account) {
                        let exhausted = acc.borrow_mut().run_pending_withdraws_to_exhaustion();
                        if !exhausted {
                            account_shard.queue_priority_account(client_account);
                        }
                    }
                }
            }

            match self.txs.try_recv() {
                Ok(transaction) => match transaction[0] {
                    Transaction::Deposit(tx) => {
                        let client_id = tx.client as u16;
                        if let Some(account) = account_shard.accounts.get_mut(&client_id) {
                            account.borrow_mut().deposit(tx);
                        } else {
                            let acc = Rc::new(RefCell::new(Account::new(client_id)));
                            acc.borrow_mut().deposit(tx);
                            account_shard.accounts.insert(client_id, acc);
                        }
                    }
                    // rather then withdrawing, queue the transaction until our system
                    // has past DISPUTE_WINDOW_MILLISECONDS
                    Transaction::PendingWithdrawal(tx) => {
                        if let Some(account) = account_shard.accounts.get_mut(&tx.client) {
                            account.borrow_mut().queue_pending_withdraw(tx);
                            account_shard.queue_pending_withdraw_account(tx.client);
                        }
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
                        dbg!("incoming tx client {:?} id {:?}!");
                        let client_id = tx.client as u16;
                        if let Some(account) = account_shard.accounts.get_mut(&client_id) {
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
    accounts: IndexMap<u16, Rc<RefCell<Account>>>,
    pending_withdraws_accs: Deque<u16, 128>,
}

impl AccountShard {
    fn new() -> Self {
        AccountShard {
            pending_withdraws_accs: Deque::new(),
            accounts: IndexMap::new(),
        }
    }

    fn queue_pending_withdraw_account(&mut self, client: u16) {
        self.pending_withdraws_accs.push_back(client).ok();
    }

    fn queue_priority_account(&mut self, client: u16) {
        self.pending_withdraws_accs.push_front(client).ok();
    }

    fn accounts_with_pending_withdraws(&mut self) -> Option<SmallVec<[u16; 10]>> {
        if self.pending_withdraws_accs.is_empty() {
            return None;
        }
        let mut accounts = SmallVec::new();
        while accounts.len() < 10 {
            if let Some(id) = self.pending_withdraws_accs.pop_front() {
                accounts.push(id);
            } else {
                break;
            }
        }
        Some(accounts)
    }
}
