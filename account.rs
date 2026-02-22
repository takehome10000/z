use crate::transaction::{DisputedTx, Transaction, Tx};
use heapless::index_map::FnvIndexMap;

use fixed::FixedI32;
use fixed::types::extra::U0;
use rust_decimal::Decimal;
use smallvec::{SmallVec, smallvec};
use std::cmp::*;

pub struct Account {
    last_tx: u64,
    locked: bool,
    earliest_tx: u64,
    client: u16,

    book: DoubleEntryBook,
    disputed_txs: FnvIndexMap<u64, DisputedTx, 100_000>,
    events: EventJournal,
}

pub struct DoubleEntryBook {
    credited: FixedI32<U0>,
    debited: FixedI32<U0>,

    pub available_funds: FixedI32<U0>,
    pub held_funds: FixedI32<U0>,
    pub total_funds: FixedI32<U0>,
}

impl DoubleEntryBook {
    fn new() -> Self {
        DoubleEntryBook {
            credited: FixedI32::<U0>::from_num(0),
            debited: FixedI32::<U0>::from_num(0),
            available_funds: FixedI32::<U0>::from_num(0),
            held_funds: FixedI32::<U0>::from_num(0),
            total_funds: FixedI32::<U0>::from_num(0),
        }
    }
}

// todo: use ordered hashmap that is stack only? why -> ideally
//   we can look up the "ordered index" if it exists its a duplicate and we drop
//   if its greater then the last index its we can continue
//   if its less then we need to find where to insert it ...
struct EventJournal {
    events: SmallVec<[Event; 1024]>,
}

impl EventJournal {
    fn new() -> Self {
        EventJournal {
            events: smallvec![],
        }
    }
    fn push(&mut self, event: Event) {
        self.events.push(event)
    }
}

struct Event {
    tx: Transaction,
}
impl Account {
    pub fn new(client: u16, tx: Transaction) -> Self {
        let mut events = EventJournal::new();
        events.push(Event { tx });
        Account {
            last_tx: 0,
            client,
            book: DoubleEntryBook::new(),
            disputed_txs: FnvIndexMap::new(),
            events,
            locked: false,
            earliest_tx: 0,
        }
    }
    pub fn locked(&self) -> bool {
        return self.locked;
    }

    pub fn book(&self) -> &DoubleEntryBook {
        &self.book
    }
    pub fn client(&self) -> u16 {
        self.client
    }

    fn ordered_exactly_once(&mut self, tx: &Tx) -> bool {
        match self.last_tx.cmp(&tx.id) {
            // if equal we have a duplicate ... return
            Ordering::Equal => return false,
            // if our last_tx is greater then our next tx replay the state
            Ordering::Greater => {
                // todo: think about this ...
                // self.replay(tx);
                return true;
            }
            // if less then we can assume all events are ordered for this account
            Ordering::Less => return true,
        }
    }

    fn deposit_internal(&mut self, tx: Tx) {
        if self.locked {
            return;
        }
    }
    fn withdraw_internal(&mut self, tx: Tx) {
        if self.locked {
            return;
        }
    }
    fn chargeback_internal(&mut self, tx: Tx) {
        self.locked = true;
    }
    fn dispute_internal(&mut self, tx: Tx) {
        self.disputed_txs
            .insert(tx.id as u64, DisputedTx { id: tx.id });
    }
    fn resolve_internal(&mut self, tx: Tx) {}

    pub fn deposit(&mut self, tx: Tx) {
        if !self.ordered_exactly_once(&tx) {
            return;
        }
        self.events.push(Event {
            tx: Transaction::Deposit(tx),
        });
        self.deposit_internal(tx);
    }

    pub fn withdraw(&mut self, tx: Tx) {
        if !self.ordered_exactly_once(&tx) {
            return;
        }
        self.last_tx = tx.id;
        self.events.push(Event {
            tx: Transaction::Withdrawal(tx),
        });
        self.withdraw_internal(tx);
    }

    pub fn dispute(&mut self, tx: Tx) {
        if !self.ordered_exactly_once(&tx) {
            return;
        }
        self.last_tx = tx.id;
        self.events.push(Event {
            tx: Transaction::Dispute(tx),
        });
        self.dispute_internal(tx);
    }

    pub fn resolve(&mut self, tx: Tx) {
        if !self.ordered_exactly_once(&tx) {
            return;
        }
        let id = tx.id;
        self.last_tx = id;
        self.events.push(Event {
            tx: Transaction::Resolve(tx),
        });
        if self.disputed_txs.get_mut(&id).is_none() {
            return;
        }
        self.resolve_internal(tx);
    }

    pub fn chargeback(&mut self, tx: Tx) {
        if !self.ordered_exactly_once(&tx) {
            return;
        }
        let id = tx.id;
        self.last_tx = id;
        self.events.push(Event {
            tx: Transaction::Resolve(tx),
        });
        if self.disputed_txs.get_mut(&id).is_some() {
            // self.events.get_mut(&id)
            // todo: place business logic here
            self.locked = true;
        }
        self.chargeback_internal(tx);
    }

    // todo: implement replay ...
    fn replay(&mut self, tx: Tx) {
        for tx in self.events.events.as_ref() {}
    }
}
