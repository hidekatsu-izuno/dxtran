//! Deterministic high-cardinality Parquet input and an independent CSV oracle.
use datafusion::arrow::{
    array::{ArrayRef, Decimal128Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};

fn mix(mut n: u64) -> u64 {
    n = (n ^ (n >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    n = (n ^ (n >> 27)).wrapping_mul(0x94d049bb133111eb);
    n ^ (n >> 31)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(args.next().expect("directory"));
    let rows: usize = args.next().expect("rows").parse()?;
    let fields = (0..8)
        .map(|j| Field::new(format!("text_{j}"), DataType::Utf8, false))
        .chain(
            (0..8).map(|j| Field::new(format!("amount_{j}"), DataType::Decimal128(16, 2), false)),
        )
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let cases = [
        ("snappy_8192", Compression::SNAPPY, 8192),
        ("zstd_65536", Compression::ZSTD(Default::default()), 65536),
    ];
    let mut writers = cases
        .iter()
        .map(|(name, codec, group)| {
            ArrowWriter::try_new(
                BufWriter::new(File::create(dir.join(format!("{name}.parquet")))?),
                schema.clone(),
                Some(
                    WriterProperties::builder()
                        .set_compression(*codec)
                        .set_max_row_group_row_count(Some(*group))
                        .build(),
                ),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut csv = BufWriter::new(File::create(dir.join("expected.csv"))?);
    for start in (0..rows).step_by(8192) {
        let end = (start + 8192).min(rows);
        let strings: Vec<Vec<String>> = (0..8)
            .map(|j| {
                (start..end)
                    .map(|n| format!("{:015x}", mix(n as u64 * 8 + j) & 0x0fff_ffff_ffff_ffff))
                    .collect()
            })
            .collect();
        let amounts: Vec<Vec<i128>> = (0..8)
            .map(|j| {
                (start..end)
                    .map(|n| (100_000_000_000 + n as i128 * 8 + j) * 100 + (n % 100) as i128)
                    .collect()
            })
            .collect();
        for row in 0..end - start {
            for column in &strings {
                write!(csv, "{},", column[row])?;
            }
            for (j, column) in amounts.iter().enumerate() {
                write!(
                    csv,
                    "{}.{:02}{}",
                    column[row] / 100,
                    column[row] % 100,
                    if j == 7 { '\n' } else { ',' }
                )?;
            }
        }
        let arrays: Vec<ArrayRef> = strings
            .iter()
            .map(|v| Arc::new(StringArray::from_iter_values(v)) as ArrayRef)
            .chain(amounts.into_iter().map(|v| {
                Arc::new(
                    Decimal128Array::from(v)
                        .with_precision_and_scale(16, 2)
                        .unwrap(),
                ) as ArrayRef
            }))
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;
        for writer in &mut writers {
            writer.write(&batch)?;
        }
    }
    csv.flush()?;
    csv.get_ref().sync_all()?;
    for writer in &mut writers {
        writer.finish()?;
        writer.inner_mut().flush()?;
        writer.inner().get_ref().sync_all()?;
    }
    drop(writers);
    let mut metadata = serde_json::Map::new();
    for (name, _, _) in cases {
        let path = dir.join(format!("{name}.parquet"));
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        metadata.insert(name.into(), serde_json::json!({"bytes":path.metadata()?.len(), "rows":rows,
            "row_groups":reader.metadata().num_row_groups(), "max_group_rows":reader.metadata().row_groups().iter().map(|g|g.num_rows()).max()}));
    }
    println!("{}", serde_json::Value::Object(metadata));
    Ok(())
}
