//! Decode row groups ahead, with bounded batches and original file ordering.
use super::{SourceTable, batch::order_key};
use crate::parallel::{check_cancelled, ordered};
use crate::{
    error::{Error, Result},
    framing,
};
use ::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use datafusion::{
    arrow::{
        array::StringBuilder,
        record_batch::{RecordBatch, RecordBatchOptions},
    },
    error::{DataFusionError, Result as DFResult},
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use std::{fs::File, sync::Arc};

pub(super) fn stream(
    source: SourceTable,
    projection: Option<Vec<usize>>,
    workers: usize,
) -> DFResult<SendableRecordBatchStream> {
    let schema = source.output_schema();
    let output_schema = Arc::new(if let Some(p) = &projection {
        schema.project(p)?
    } else {
        (*schema).clone()
    });
    let source = Arc::new(source);
    let init_source = source.clone();
    let groups = stream::once(async move {
        tokio::task::spawn_blocking(move || {
            let file =
                File::open(&init_source.dataset.path).map_err(|e| Error::input(e.to_string()))?;
            if !file
                .metadata()
                .map_err(|e| Error::input(e.to_string()))?
                .is_file()
            {
                return Err(Error::config("input must be a regular file"));
            }
            let metadata = ArrowReaderMetadata::load(&file, Default::default())
                .map_err(|e| Error::input(e.to_string()))?;
            if metadata.schema() != &init_source.layout.schema {
                return Err(Error::input("schema changed after planning"));
            }
            // Match the serial reader's file-wide batch size. Reassemble
            // partial groups below so downstream writers see the same batch
            // boundaries regardless of concurrency (including flush points).
            let bytes = metadata
                .metadata()
                .row_groups()
                .iter()
                .fold(0u64, |n, group| {
                    n.saturating_add(group.total_byte_size().max(0) as u64)
                });
            let total_rows = metadata.metadata().file_metadata().num_rows().max(1) as u64;
            let width = bytes.div_ceil(total_rows).max(1);
            let rows = (framing::MAX_RECORD as u64 / width).clamp(1, 8192) as usize;
            let mut first = 0u64;
            let groups: Vec<_> = metadata
                .metadata()
                .row_groups()
                .iter()
                .enumerate()
                .map(|(index, group)| {
                    let offset = first;
                    first += group.num_rows().max(0) as u64;
                    Ok((metadata.clone(), index, offset, rows))
                })
                .collect();
            Ok(stream::iter(groups))
        })
        .await
        .map_err(|e| Error::input(e.to_string()))?
    })
    .try_flatten();
    let merge_budget = source.conversion_budget.clone();
    let decoded = ordered(
        groups,
        workers,
        move |(metadata, index, first, rows), tx, cancelled| {
            let source = source.clone();
            let schema = schema.clone();
            let projection = projection.clone();
            async move {
                let mut reader = None;
                let mut record = first;
                loop {
                    if tx.is_closed() {
                        return Ok(());
                    }
                    let permit = source
                        .conversion_budget
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|e| Error::sql(e.to_string()))?;
                    let source = source.clone();
                    let schema = schema.clone();
                    let projection = projection.clone();
                    let metadata = metadata.clone();
                    let cancelled = cancelled.clone();
                    let (next_reader, batch) = tokio::task::spawn_blocking(move || -> Result<_> {
                        let _permit = permit;
                        let context = |e: Error| {
                            e.context(format!(
                                "input {} {} record {} row group {}",
                                source.dataset.id,
                                source.dataset.path.display(),
                                record + 1,
                                index + 1
                            ))
                        };
                        check_cancelled(&cancelled)?;
                        let mut reader = match reader {
                            Some(reader) => reader,
                            None => ParquetRecordBatchReaderBuilder::new_with_metadata(
                                File::open(&source.dataset.path)
                                    .map_err(|e| context(Error::input(e.to_string())))?,
                                metadata,
                            )
                            .with_row_groups(vec![index])
                            .with_batch_size(rows)
                            .build()
                            .map_err(|e| context(Error::input(e.to_string())))?,
                        };
                        let batch = reader.next().transpose().map_err(|e| {
                            context(Error::input(format!("parquet decoding failed: {e}")))
                        })?;
                        let batch = batch
                            .map(|batch| {
                                let mut columns = batch.columns().to_vec();
                                if source.order_name.is_some() {
                                    let mut keys = StringBuilder::with_capacity(
                                        batch.num_rows(),
                                        batch.num_rows() * 32,
                                    );
                                    for row in 1..=batch.num_rows() {
                                        let key = order_key(source.dataset_no, record + row as u64);
                                        keys.append_value(
                                            std::str::from_utf8(&key).expect("ASCII order key"),
                                        );
                                    }
                                    columns.push(Arc::new(keys.finish()));
                                }
                                let batch = RecordBatch::try_new_with_options(
                                    schema,
                                    columns,
                                    &RecordBatchOptions::new()
                                        .with_row_count(Some(batch.num_rows())),
                                )
                                .map_err(DataFusionError::from)?;
                                Ok::<_, Error>(if let Some(p) = projection {
                                    batch.project(&p).map_err(DataFusionError::from)?
                                } else {
                                    batch
                                })
                            })
                            .transpose()?;
                        Ok((reader, batch))
                    })
                    .await
                    .map_err(|e| Error::input(e.to_string()))??;
                    reader = Some(next_reader);
                    let Some(batch) = batch else {
                        return Ok(());
                    };
                    record += batch.num_rows() as u64;
                    if tx.send((batch, rows)).await.is_err() {
                        return Ok(());
                    }
                }
            }
        },
    );
    let merge_schema = output_schema.clone();
    let decoded = stream::try_unfold(
        (Box::pin(decoded.fuse()), None::<(RecordBatch, usize)>),
        move |(mut batches, mut pending)| {
            let schema = merge_schema.clone();
            let budget = merge_budget.clone();
            async move {
                let mut parts = Vec::new();
                let mut count = 0;
                loop {
                    let next = match pending.take() {
                        Some(batch) => Some(batch),
                        None => batches.next().await.transpose()?,
                    };
                    let Some((batch, target)) = next else {
                        break;
                    };
                    let take = batch.num_rows().min(target - count);
                    if take < batch.num_rows() {
                        pending = Some((batch.slice(take, batch.num_rows() - take), target));
                        parts.push(batch.slice(0, take));
                    } else {
                        parts.push(batch);
                    }
                    count += take;
                    if count == target {
                        break;
                    }
                }
                if parts.is_empty() {
                    return Ok::<_, Error>(None);
                }
                let batch = if parts.len() == 1 {
                    parts.pop().unwrap()
                } else {
                    let permit = budget
                        .acquire_owned()
                        .await
                        .map_err(|e| Error::sql(e.to_string()))?;
                    tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        datafusion::arrow::compute::concat_batches(&schema, &parts)
                            .map_err(DataFusionError::from)
                    })
                    .await
                    .map_err(|e| Error::input(e.to_string()))??
                };
                Ok(Some((batch, (batches, pending))))
            }
        },
    )
    .map_err(|e| DataFusionError::External(Box::new(e)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        decoded,
    )))
}
