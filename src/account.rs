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
    pub fn new(client: u16, tx: Transaction) -> Self {
        Account {
            client,
            book: DoubleEntryBook::new(),
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

    pub fn deposit(&mut self, tx: Tx) {
        // round up to save the bank money
        let amount = tx
            .amount
            .round_dp_with_strategy(4, RoundingStrategy::AwayFromZero);

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

    pub fn withdraw(&mut self, tx: Tx) {
        // round down to save the bank money
        let amount = tx
            .amount
            .round_dp_with_strategy(4, RoundingStrategy::ToZero);
        println!("withdraw amount is {:?}", amount);

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
            return;
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
            return;
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
