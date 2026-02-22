use crate::transaction::{Transaction, Tx};
use arrow::array::{Array, StringArray, UInt16Array, UInt64Array};
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use std::fs::File;
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime::Runtime;

const TX_CHUNK_SIZE: usize = 30000;

lazy_static! {
    static ref CSV_SCHEMA_INPUT: Schema = Schema::new(vec![
        Field::new("type", DataType::Utf8, false),
        Field::new("client", DataType::UInt16, false),
        Field::new("tx", DataType::UInt64, false),
        Field::new("amount", DataType::Utf8, true),
    ]);
}

pub struct ConcurrentAsyncFileDescriptorReader {
    rt: Runtime,
    senders: Vec<crossbeam::channel::Sender<Transaction>>,
}

impl ConcurrentAsyncFileDescriptorReader {
    pub fn new(senders: Vec<crossbeam::channel::Sender<Transaction>>) -> Self {
        let rt = Runtime::new().expect("failed to create tokio runtime");
        Self { rt, senders }
    }

    pub fn consume(&self, files: Vec<String>) -> anyhow::Result<()> {
        self.rt.block_on(async {
            let mut handles = vec![];
            let senders = self.senders.clone();
            for file in files {
                let senders = senders.clone();
                let handle = tokio::spawn(async move {
                    let file = File::open(&file)?;
                    let schema = Arc::new(CSV_SCHEMA_INPUT.clone());
                    let mut reader = ReaderBuilder::new(schema)
                        .with_header(true)
                        .with_batch_size(TX_CHUNK_SIZE)
                        .build(file)?;
                    while let Some(batch) = reader.next() {
                        let batch = batch?;
                        let types = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        let clients = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<UInt16Array>()
                            .unwrap();
                        let ids = batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let amounts = batch
                            .column(3)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        for i in 0..batch.num_rows() {
                            let intent = types.value(i);
                            let client = clients.value(i);
                            let id = ids.value(i);
                            let amount = if amounts.is_null(i) {
                                Decimal::ZERO
                            } else {
                                match Decimal::from_str(amounts.value(i).trim()) {
                                    Ok(d) => d,
                                    Err(_) => {
                                        eprintln!("skipping bad amount: {}", amounts.value(i));
                                        continue;
                                    }
                                }
                            };
                            let tx = Tx { client, id, amount };
                            let tx = match intent.trim() {
                                "deposit" => Transaction::Deposit(tx),
                                "withdrawal" => Transaction::Withdrawal(tx),
                                "dispute" => Transaction::Dispute(tx),
                                "resolve" => Transaction::Resolve(tx),
                                "chargeback" => Transaction::Chargeback(tx),
                                _ => {
                                    eprintln!("skipping unknown intent: {}", intent);
                                    continue;
                                }
                            };
                            let shard_idx = client as usize % senders.len();
                            if let Err(e) = senders[shard_idx].send(tx) {
                                eprintln!("shard send failed: {}", e);
                            }
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                });
                handles.push(handle);
            }
            for handle in handles {
                handle.await??;
            }
            Ok(())
        })
    }
}
