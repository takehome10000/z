use crate::account::*;
use arrow::array::{BooleanArray, StringArray, UInt16Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_csv::writer::WriterBuilder;
use lazy_static::lazy_static;
use std::io::stdout;
use std::sync::Arc;

lazy_static! {
    static ref CSV_SCHEMA_OUTPUT: Schema = Schema::new(vec![
        Field::new("client", DataType::UInt16, false),
        Field::new("available", DataType::Decimal64(10, 4), false),
        Field::new("held", DataType::Decimal64(10, 4), false),
        Field::new("total", DataType::Decimal64(10, 4), false),
        Field::new("locked", DataType::Boolean, true),
    ]);
}

struct AccountOutput {
    client: u16,
    available: u64,
    held: u64,
    total: u64,
    locked: bool,
}

pub fn output_accounts(shards: Vec<*const Vec<Account>>) -> anyhow::Result<()> {
    let mut clients: Vec<u16> = vec![];
    let mut available: Vec<String> = vec![];
    let mut held: Vec<String> = vec![];
    let mut total: Vec<String> = vec![];
    let mut locked: Vec<bool> = vec![];

    for ptr in shards {
        let accounts = unsafe { &*ptr };
        for account in accounts {
            clients.push(account.client());
            let book = account.book();
            available.push(format!("{:.4}", book.available_funds));
            held.push(format!("{:.4}", book.held_funds));
            total.push(format!("{:.4}", book.total_funds));
            locked.push(account.locked());
        }
    }

    let batch = RecordBatch::try_new(
        Arc::new(CSV_SCHEMA_OUTPUT.clone()),
        vec![
            Arc::new(UInt16Array::from(clients)),
            Arc::new(StringArray::from(available)),
            Arc::new(StringArray::from(held)),
            Arc::new(StringArray::from(total)),
            Arc::new(arrow::array::BooleanArray::from(locked)),
        ],
    )?;

    let mut writer = WriterBuilder::new().with_header(true).build(stdout());

    writer.write(&batch)?;

    Ok(())
}
