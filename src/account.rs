use crate::output::AccountOutput;
use crate::transaction::Tx;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::collections::{HashMap, HashSet};

pub struct Account {
    locked: bool,
    client: u16,
    book: DoubleEntryBook,
    disputed_txs: HashSet<u32>,
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
            disputed_txs: HashSet::new(),
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

    pub fn deposit(&mut self, tx: Tx) {
        let amount = tx
            .amount
            .round_dp_with_strategy(4, RoundingStrategy::ToZero);

        if amount.is_zero() {
            return;
        }
        if amount.is_negative() {
            return;
        }
        if self.locked {
            return;
        }
        if self.deposits.contains_key(&tx.id) {
            return;
        }

        self.book.available_funds += amount;
        self.book.total_funds += amount;
        self.deposits.insert(tx.id, amount);
    }

    pub fn withdraw(&mut self, tx: Tx) {
        let amount = tx.amount.round_dp(4);

        if amount.is_zero() {
            return;
        }
        if amount.is_negative() {
            return;
        }
        if self.locked {
            return;
        }
        if self.withdraws.contains_key(&tx.id) {
            return;
        }
        if (self.book.available_funds - amount).is_negative() {
            return;
        }

        self.book.available_funds -= amount;
        self.book.total_funds -= amount;
        self.withdraws.insert(tx.id, amount);
    }

    pub fn dispute(&mut self, tx: Tx) {
        if self.disputed_txs.contains(&tx.id) {
            return;
        }

        if let Some(&amount) = self.deposits.get(&tx.id) {
            self.book.available_funds -= amount;
            self.book.held_funds += amount;
            self.disputed_txs.insert(tx.id);
        }
    }

    pub fn resolve(&mut self, tx: Tx) {
        if !self.disputed_txs.contains(&tx.id) {
            return;
        }

        if let Some(&amount) = self.deposits.get(&tx.id) {
            self.disputed_txs.remove(&tx.id);
            self.book.held_funds -= amount;
            self.book.available_funds += amount;
        }
    }

    pub fn chargeback(&mut self, tx: Tx) {
        if !self.disputed_txs.contains(&tx.id) {
            return;
        }

        if let Some(&amount) = self.deposits.get(&tx.id) {
            self.disputed_txs.remove(&tx.id);
            self.book.held_funds -= amount;
            self.book.total_funds -= amount;
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
