use std::env;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::thread::sleep;

mod account;
mod engine;
mod io;
mod output;
mod shard;
mod transaction;

use anyhow::anyhow;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_csv::reader::Format;
use engine::Engine;
use io::ConcurrentAsyncFileDescriptorReader;
use output::{AccountOutput, write_output_accounts};
use std::fs::File;
use std::path::Path;

fn is_csv(path: &str) -> anyhow::Result<()> {
    let mut file = File::open(path)?;
    let format = Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut file, Some(10))?;

    let expected = Schema::new(vec![
        Field::new("type", DataType::Utf8, false),
        Field::new("client", DataType::UInt16, false),
        Field::new("tx", DataType::UInt32, false),
        Field::new("amount", DataType::Decimal128(10, 4), true),
    ]);

    for field in expected.fields() {
        match schema.field_with_name(field.name()) {
            Ok(_) => {}
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "invalid csv schema in given file: {}",
                    path
                ));
            }
        }
    }

    Ok(())
}

fn resolve_path(path: &str) -> anyhow::Result<String> {
    let p = Path::new(path);
    if p.is_absolute() {
        Ok(path.to_string())
    } else {
        let current = env::current_dir()?;
        Ok(current.join(p).to_string_lossy().to_string())
    }
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    let filename = args[1].clone();
    is_csv(filename.as_str())?;
    resolve_path(filename.as_str())?;

    dbg!("consuming file {:?}", &filename);

    let done = Arc::new(AtomicBool::new(false));

    let num_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .map_err(|e| anyhow!("failed to get available cores {:?}", e))?;

    let (engine, senders) = Engine::new(num_workers, done.clone())?;
    let handler = std::thread::spawn(move || -> anyhow::Result<()> {
        let oas = engine.run()?;
        write_output_accounts(oas)?;
        Ok(())
    });
    let reader_io = ConcurrentAsyncFileDescriptorReader::new(senders);
    let files = vec![filename];
    reader_io.consume(files)?;
    sleep(std::time::Duration::from_micros(5000));
    done.store(true, SeqCst);
    handler.join();
    Ok(())
}
