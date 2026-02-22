use crate::output::AccountOutput;
use crate::transaction::{DisputedTx, Transaction, Tx};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::collections::HashMap;

pub struct Account {
    locked: bool,
    client: u16,
    book: DoubleEntryBook,
    disputed_txs: HashMap<u32, DisputedTx>,
    deposits: HashMap<u32, Decimal>,
}

pub struct DoubleEntryBook {
    credited: Decimal,
    debited: Decimal,
    pub available_funds: Decimal,
    pub held_funds: Decimal,
    pub total_funds: Decimal,
}

impl DoubleEntryBook {
    fn new() -> Self {
        DoubleEntryBook {
            credited: Decimal::ZERO,
            debited: Decimal::ZERO,
            available_funds: Decimal::ZERO,
            held_funds: Decimal::ZERO,
            total_funds: Decimal::ZERO,
        }
    }
}

impl Account {
    pub fn new(client: u16, tx: Transaction) -> Self {
        Account {
            client,
            book: DoubleEntryBook::new(),
            disputed_txs: HashMap::new(),
            deposits: HashMap::new(),
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
        if self.locked {
            return;
        }
        let amount = tx.amount.round_dp(4);
        self.deposits.insert(tx.id as u32, amount);
        self.book.available_funds += amount;
        self.book.total_funds += amount;
        self.book.credited += amount;
    }

    pub fn withdraw(&mut self, tx: Tx) {
        if self.locked {
            return;
        }
        let amount = tx.amount.round_dp(4);
        if self.book.available_funds < amount {
            return;
        }
        self.book.available_funds -= amount;
        self.book.total_funds -= amount;
        self.book.debited += amount;
    }

    pub fn dispute(&mut self, tx: Tx) {
        let Some(&amount) = self.deposits.get(&(tx.id as u32)) else {
            return;
        };
        if self.disputed_txs.contains_key(&(tx.id as u32)) {
            return;
        }
        self.disputed_txs
            .insert(tx.id as u32, DisputedTx { id: tx.id as u32 });
        self.book.available_funds -= amount;
        self.book.held_funds += amount;
    }

    pub fn resolve(&mut self, tx: Tx) {
        if !self.disputed_txs.contains_key(&(tx.id as u32)) {
            return;
        }
        let Some(&amount) = self.deposits.get(&(tx.id as u32)) else {
            return;
        };
        self.disputed_txs.remove(&(tx.id as u32));
        self.book.held_funds -= amount;
        self.book.available_funds += amount;
    }

    pub fn chargeback(&mut self, tx: Tx) {
        if !self.disputed_txs.contains_key(&(tx.id as u32)) {
            return;
        }
        let Some(&amount) = self.deposits.get(&(tx.id as u32)) else {
            return;
        };
        self.disputed_txs.remove(&(tx.id as u32));
        self.book.held_funds -= amount;
        self.book.total_funds -= amount;
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
