use crate::output::AccountOutput;
use crate::transaction::{DisputedTx, Transaction, Tx};
use heapless::spsc::Queue;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use smallvec::{SmallVec, smallvec};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

const DISPUTE_WINDOW_MILLISECONDS: u128 = 1; // 1 to test for now
const PENDING_WITHDRAWS_SIZE: u64 = 100;

struct PendingWithdraw {
    tx: Tx,
    arrival_time: u128,
}

pub struct PendingWithdraws {
    max_released: u16,
    inner: Queue<PendingWithdraw, 256>,
}

impl PendingWithdraws {
    fn new() -> Self {
        PendingWithdraws {
            max_released: 8,
            inner: Queue::new(),
        }
    }

    fn queue(&mut self, pw: PendingWithdraw) {
        self.inner.enqueue(pw).ok();
    }

    fn release_ready(&mut self) -> Option<SmallVec<[PendingWithdraw; 8]>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis();

        let mut ready = SmallVec::new();

        while let Some(pw) = self.inner.peek() {
            if now >= pw.arrival_time + DISPUTE_WINDOW_MILLISECONDS {
                if let Some(pw) = self.inner.dequeue() {
                    ready.push(pw);
                }
            } else {
                break;
            }
        }

        Some(ready)
    }
}

pub struct Account {
    locked: bool,
    client: u16,
    book: DoubleEntryBook,
    pending_withdraws: PendingWithdraws,
    disputed_txs: HashMap<u32, DisputedTx>,
    deposits: HashMap<u32, Decimal>,
    withdraws: HashMap<u32, Decimal>,
}

pub struct DoubleEntryBook {
    pub available_funds: Decimal,
    pub held_funds: Decimal,
    pub total_funds: Decimal,
}

impl DoubleEntryBook {
    fn new() -> Self {
        DoubleEntryBook {
            available_funds: Decimal::ZERO,
            held_funds: Decimal::ZERO,
            total_funds: Decimal::ZERO,
        }
    }
}

impl Account {
    pub fn new(client: u16) -> Self {
        Account {
            client,
            book: DoubleEntryBook::new(),
            pending_withdraws: PendingWithdraws::new(),
            disputed_txs: HashMap::new(),
            deposits: HashMap::new(),
            withdraws: HashMap::new(),
            locked: false,
        }
    }

    pub fn locked(&self) -> bool {
        self.locked
    }

    pub fn book(&self) -> &DoubleEntryBook {
        &self.book
    }

    pub fn client(&self) -> u16 {
        self.client
    }

    // run_pending_windraws_to_exhaustion runs withdraw txs - if we
    // exhaust the list we return true
    pub fn run_pending_withdraws_to_exhaustion(&mut self) -> bool {
        let Some(pending_withdraws) = self.pending_withdraws.release_ready() else {
            return false;
        };
        if pending_withdraws.is_empty() {
            return false;
        }

        dbg!("z");

        for pw in pending_withdraws {
            self.withdraw(pw.tx);
        }

        true
    }

    pub fn deposit(&mut self, tx: Tx) {
        // round down to save the bank money
        let amount = tx
            .amount
            .round_dp_with_strategy(4, RoundingStrategy::ToZero);

        let is_negative = amount.is_negative();
        let locked = self.locked;
        let duplicate = self.deposits.contains_key(&tx.id);
        let is_zero = amount.is_zero();

        if locked {
            return;
        }
        if duplicate {
            return;
        }
        if is_negative {
            return;
        }
        if is_zero {
            return;
        }

        self.book.available_funds += amount;
        self.book.total_funds += amount;
        self.deposits.insert(tx.id, amount);
    }

    pub fn queue_pending_withdraw(&mut self, tx: Tx) {
        let Some(arrival_time) = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_millis())
        else {
            return;
        };

        let pw = PendingWithdraw { tx, arrival_time };

        self.pending_withdraws.queue(pw);
    }

    pub fn withdraw(&mut self, tx: Tx) {
        let amount = tx.amount.round_dp(4);

        let is_negative = tx.amount.is_negative();
        let locked = self.locked;
        let duplicate = self.withdraws.contains_key(&tx.id);
        let insufficient_funds = (self.book.available_funds - amount).is_negative();
        let is_zero = amount.is_zero();

        if locked {
            return;
        }
        if duplicate {
            return;
        }
        if is_negative {
            return;
        }
        if is_zero {
            return;
        }
        if insufficient_funds {
            return;
        }
        if is_zero {
            return;
        }

        self.book.available_funds -= amount;
        self.book.total_funds -= amount;
        self.withdraws.insert(tx.id, amount);
    }

    // todo: should disputes be made a priority over withdraw transactions?
    pub fn dispute(&mut self, tx: Tx) {
        if self.disputed_txs.contains_key(&tx.id) {
            return;
        }
        if let Some(&amount) = self.deposits.get(&tx.id) {
            self.book.available_funds -= amount;
            self.book.held_funds += amount;
            self.disputed_txs.insert(tx.id, DisputedTx { id: tx.id });
        } else if let Some(&amount) = self.withdraws.get(&tx.id) {
            self.book.available_funds -= amount;
            self.book.held_funds += amount;
            self.disputed_txs.insert(tx.id, DisputedTx { id: tx.id });
        }
    }

    pub fn resolve(&mut self, tx: Tx) {
        if !self.disputed_txs.contains_key(&tx.id) {
            return;
        }
        if let Some(&amount) = self.deposits.get(&tx.id) {
            self.disputed_txs.remove(&tx.id);
            self.book.held_funds -= amount;
            self.book.available_funds += amount;
        } else if let Some(&amount) = self.withdraws.get(&tx.id) {
            self.disputed_txs.remove(&tx.id);
            self.book.held_funds -= amount;
            self.book.available_funds += amount;
        }
    }

    pub fn chargeback(&mut self, tx: Tx) {
        if !self.disputed_txs.contains_key(&tx.id) {
            return;
        }
        if let Some(&amount) = self.deposits.get(&tx.id) {
            self.disputed_txs.remove(&tx.id);
            self.book.held_funds -= amount;
            self.book.total_funds -= amount;
        } else if let Some(&amount) = self.withdraws.get(&tx.id) {
            self.disputed_txs.remove(&tx.id);
            self.book.held_funds -= amount;
            self.book.total_funds += amount;
        }
        self.locked = true;
    }
}

impl Into<AccountOutput> for Account {
    fn into(self) -> AccountOutput {
        let book = self.book();
        AccountOutput {
            client: self.client,
            total: book.total_funds,
            available: book.available_funds,
            held: book.held_funds,
            locked: self.locked,
        }
    }
}
