use super::*;
use crate::{field::FieldType, parallel::check_cancelled};
use std::sync::atomic::AtomicBool;

pub(crate) struct PhysicalEncoder {
    id: String,
    format: Format,
    layout: RecordLayout,
    indices: Vec<Vec<usize>>,
}
impl PhysicalEncoder {
    pub(super) fn new(dataset: &Dataset, layout: &RecordLayout, indices: &[Vec<usize>]) -> Self {
        Self {
            id: dataset.id.clone(),
            format: dataset.format,
            layout: layout.clone(),
            indices: indices.to_vec(),
        }
    }
    pub(crate) fn batch_rows(&self) -> usize {
        (framing::MAX_RECORD / (self.layout.length + self.layout.separator.len())).clamp(1, 8192)
    }
    pub(crate) fn encode_fixed(
        &self,
        batch: &RecordBatch,
        start_row: u64,
        cancelled: &AtomicBool,
    ) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(
            batch.num_rows() * (self.layout.length + self.layout.separator.len()),
        );
        let mut rows = start_row;
        self.write_batch(batch, &mut bytes, &mut rows, Some(cancelled))?;
        Ok(bytes)
    }
    pub(super) fn write_batch(
        &self,
        batch: &RecordBatch,
        writer: &mut impl Write,
        rows: &mut u64,
        cancelled: Option<&AtomicBool>,
    ) -> Result<()> {
        let mut body = Vec::with_capacity(self.layout.length);
        let mut bytes = Vec::new();
        let mut values = Vec::new();
        for row in 0..batch.num_rows() {
            if row % 256 == 0
                && let Some(cancelled) = cancelled
            {
                check_cancelled(cancelled)?;
            }
            body.clear();
            body.resize(self.layout.length, 0);
            for (f, indices) in self.layout.fields.iter().zip(&self.indices) {
                let result = (|| -> Result<()> {
                    if f.kind == FieldType::String {
                        let column = batch.column(indices[0]);
                        if !column.is_null(row) {
                            let text = match column.data_type() {
                                DataType::Utf8 => Some(column.as_string::<i32>().value(row)),
                                DataType::LargeUtf8 => Some(column.as_string::<i64>().value(row)),
                                DataType::Utf8View => Some(column.as_string_view().value(row)),
                                _ => None,
                            };
                            if let Some(text) = text {
                                f.encode_string_into(text, &mut bytes)?;
                                place_field(&mut body, f.offset, &bytes);
                                return Ok(());
                            }
                        }
                    }
                    if f.kind.decimal() && f.kind != FieldType::Decimal {
                        let column = batch.column(indices[0]);
                        if let DataType::Decimal128(_, scale) = column.data_type() {
                            let array = column.as_primitive::<Decimal128Type>();
                            if !array.is_null(row) {
                                f.encode_decimal_into(array.value(row), *scale, &mut bytes)?;
                                place_field(&mut body, f.offset, &bytes);
                                return Ok(());
                            }
                        }
                    }
                    values.clear();
                    for i in indices {
                        let column = batch.column(*i);
                        let value = if let DataType::Decimal128(p, s) = column.data_type() {
                            let array = column.as_primitive::<Decimal128Type>();
                            ScalarValue::Decimal128(
                                (!array.is_null(row)).then(|| array.value(row)),
                                *p,
                                *s,
                            )
                        } else {
                            ScalarValue::try_from_array(column, row).map_err(Error::from)?
                        };
                        values.push(value);
                    }
                    f.encode_into(&values, self.format, &mut bytes)?;
                    place_field(&mut body, f.offset, &bytes);
                    Ok(())
                })();
                result.map_err(|e| {
                    e.context(format!(
                        "output {} record {} column {} expected {:?} width {:?}",
                        self.id,
                        *rows + 1,
                        f.spec.name.as_deref().unwrap_or("FILLER"),
                        f.kind,
                        f.length
                    ))
                })?;
            }
            if self.format == Format::Rdw {
                framing::write_rdw(writer, &body, self.layout.max_record_length)?;
            } else {
                writer
                    .write_all(&body)
                    .and_then(|_| writer.write_all(&self.layout.separator))
                    .map_err(|e| Error::output(e.to_string()))?;
            }
            *rows += 1;
        }
        Ok(())
    }
}
