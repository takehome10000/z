use crate::transaction::{Transaction, Tx};
use arrow::array::{Array, StringArray, UInt16Array, UInt32Array};
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_csv::reader::Decoder;
use futures::Stream;
use futures::TryStreamExt;
use futures::ready;
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use tokio::runtime::Runtime;

const TX_CHUNK_SIZE: usize = 300_000;

lazy_static! {
    static ref CSV_SCHEMA_INPUT: Schema = Schema::new(vec![
        Field::new("type", DataType::Utf8, false),
        Field::new("client", DataType::UInt16, false),
        Field::new("tx", DataType::UInt32, false),
        Field::new("amount", DataType::Utf8, true),
    ]);
}

pub struct ConcurrentAsyncFileDescriptorReader {
    rt: Runtime,
    senders: Vec<crossbeam::channel::Sender<Vec<Transaction>>>,
}

// note: decode_stream is pulled from here https://docs.rs/arrow-csv/latest/arrow_csv/reader/
fn decode_stream<R: AsyncBufRead + Unpin>(
    mut decoder: Decoder,
    mut reader: R,
) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
    futures::stream::poll_fn(move |cx| {
        loop {
            let b = match ready!(Pin::new(&mut reader).poll_fill_buf(cx)) {
                Ok(b) => b,
                Err(e) => return Poll::Ready(Some(Err(e.into()))),
            };
            let decoded = match decoder.decode(b) {
                // note: the decoder needs to be called with an empty
                // array to delimit the final record
                Ok(0) => break,
                Ok(decoded) => decoded,
                Err(e) => return Poll::Ready(Some(Err(e))),
            };
            Pin::new(&mut reader).consume(decoded);
        }

        Poll::Ready(decoder.flush().transpose())
    })
}

impl ConcurrentAsyncFileDescriptorReader {
    pub fn new(senders: Vec<crossbeam::channel::Sender<Vec<Transaction>>>) -> Self {
        let rt = Runtime::new().expect("failed to create tokio runtime");
        Self { rt, senders }
    }

    pub fn consume(&self, files: Vec<String>) -> anyhow::Result<()> {
        self.rt.block_on(async {
            let mut handles = vec![];
            let senders = self.senders.clone();
            for f in files {
                let senders = senders.clone();
                let handle = tokio::spawn(async move {
                    let file = tokio::fs::File::open(f).await?;
                    let reader = tokio::io::BufReader::new(file);
                    let decoder = ReaderBuilder::new(Arc::new(CSV_SCHEMA_INPUT.clone()))
                        .with_header(true)
                        .with_batch_size(TX_CHUNK_SIZE)
                        .build_decoder();
                    let mut stream = decode_stream(decoder, reader);

                    while let Some(batch) = stream.try_next().await? {
                        // todo: is there a nicer way of doing this type casting / object tx serialization
                        //       in arrow?
                        //
                        // https://github.com/apache/arrow-rs/issues/1760

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
                            .downcast_ref::<UInt32Array>()
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
                                        println!("skipping bad amount: {}", amounts.value(i));
                                        continue;
                                    }
                                }
                            };
                            let tx = Tx { client, id, amount };
                            let tx = match intent.trim() {
                                "deposit" => Transaction::Deposit(tx),
                                "withdraw" => Transaction::Withdrawal(tx),
                                "dispute" => Transaction::Dispute(tx),
                                "resolve" => Transaction::Resolve(tx),
                                "chargeback" => Transaction::Chargeback(tx),
                                _ => {
                                    println!("skipping unknown intent: {}", intent);
                                    continue;
                                }
                            };
                            let shard_idx = client as usize % senders.len();
                            if let Err(e) = senders[shard_idx].send(vec![tx]) {
                                println!("shard send failed: {}", e);
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
