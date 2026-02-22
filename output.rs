use crate::account::*;
use arrow::array::{BooleanArray, StringArray, UInt16Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_csv::writer::WriterBuilder;
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use std::io::stdout;
use std::sync::Arc;

lazy_static! {
    static ref CSV_SCHEMA_OUTPUT: Schema = Schema::new(vec![
        Field::new("client", DataType::UInt16, false),
        Field::new("available", DataType::Utf8, false),
        Field::new("held", DataType::Utf8, false),
        Field::new("total", DataType::Utf8, false),
        Field::new("locked", DataType::Boolean, true),
    ]);
}

pub struct AccountOutput {
    pub client: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}

pub fn write_output_accounts(shards: Vec<AccountOutput>) -> anyhow::Result<()> {
    let mut clients: Vec<u16> = vec![];
    let mut available: Vec<String> = vec![];
    let mut held: Vec<String> = vec![];
    let mut total: Vec<String> = vec![];
    let mut locked: Vec<bool> = vec![];

    shards.iter().for_each(|account| {
        clients.push(account.client);
        available.push(account.available.to_string());
        held.push(account.held.to_string());
        total.push(account.total.to_string());
        locked.push(account.locked);
    });

    let batch = RecordBatch::try_new(
        Arc::new(CSV_SCHEMA_OUTPUT.clone()),
        vec![
            Arc::new(UInt16Array::from(clients)),
            Arc::new(StringArray::from(available)),
            Arc::new(StringArray::from(held)),
            Arc::new(StringArray::from(total)),
            Arc::new(BooleanArray::from(locked)),
        ],
    )?;

    let mut writer = WriterBuilder::new().with_header(true).build(stdout());
    writer.write(&batch)?;
    Ok(())
}
