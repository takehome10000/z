use rust_decimal::Decimal;

#[derive(Debug, Copy, Clone)]
pub struct Tx {
    pub client: u16,
    pub id: u32,
    pub amount: Decimal,
}

#[derive(Debug)]
pub enum Transaction {
    Deposit(Tx),
    PendingWithdrawal(Tx),
    Dispute(Tx),
    Resolve(Tx),
    Chargeback(Tx),
}

pub struct PendingWithdraw {
    pub arrival_time: u64,
    pub tx: Tx,
}
