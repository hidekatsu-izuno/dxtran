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
    pub(super) fn worthwhile(&self) -> bool {
        crate::parallel::worthwhile(&self.layout)
    }
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
        self.write_batch(batch, &mut bytes, &mut rows, Some(cancelled), None)?;
        Ok(bytes)
    }
    pub(super) fn write_batch(
        &self,
        batch: &RecordBatch,
        writer: &mut impl Write,
        rows: &mut u64,
        cancelled: Option<&AtomicBool>,
        mut rdw: Option<&mut framing::rdw::RdwWriter>,
    ) -> Result<()> {
        let mut body = Vec::with_capacity(self.layout.length);
        let mut bytes = Vec::new();
        let mut values = Vec::new();
        let all_fields: Vec<_> = (0..self.layout.fields.len()).collect();
        let mut selection = crate::layout::SelectionCache::default();
        let checked = (!self.layout.checks.is_empty())
            .then(|| self.layout.checks.project(batch))
            .transpose()?;
        let predicates = self
            .layout
            .conditional
            .as_ref()
            .filter(|_| checked.is_none())
            .map(|c| c.evaluate(batch))
            .transpose()
            .map_err(|e| {
                e.context(format!(
                    "output {} record {}..{}",
                    self.id,
                    *rows + 1,
                    *rows + batch.num_rows() as u64
                ))
            })?;
        for row in 0..batch.num_rows() {
            if let Some(checked) = &checked {
                self.layout.checks.check_row(checked, row, *rows + 1)?;
            }
            if row % 256 == 0
                && let Some(cancelled) = cancelled
            {
                check_cancelled(cancelled)?;
            }
            let row_predicates = if checked.is_some() {
                self.layout
                    .conditional
                    .as_ref()
                    .map(|c| c.evaluate(&batch.slice(row, 1)))
                    .transpose()
                    .map_err(|e| e.context(format!("output {} record {}", self.id, *rows + 1)))?
            } else {
                None
            };
            let selected = self
                .layout
                .conditional
                .as_ref()
                .map(|c| {
                    c.select(
                        &self.layout,
                        row_predicates
                            .as_ref()
                            .or(predicates.as_ref())
                            .expect("conditional predicates"),
                        if checked.is_some() { 0 } else { row },
                        &mut selection,
                    )
                })
                .transpose()
                .map_err(|e| e.context(format!("output {} record {}", self.id, *rows + 1)))?;
            body.clear();
            body.resize(
                selected.as_ref().map_or(self.layout.length, |s| s.length),
                0,
            );
            let active = selected
                .as_ref()
                .map_or(all_fields.as_slice(), |s| &s.fields);
            for &i in active {
                let f = &self.layout.fields[i];
                let indices = &self.indices[i];
                let result = (|| -> Result<()> {
                    if f.kind == FieldType::String {
                        let column = batch.column(indices[0]);
                        if !column.is_null(row) {
                            let text = crate::arrow_value::text(column.as_ref(), row);
                            if let Some(text) = text {
                                f.encode_string_into(text, &mut bytes)?;
                                place_field(&mut body, f.offset, &bytes);
                                return Ok(());
                            }
                        }
                    }
                    if matches!(f.kind, FieldType::Binary | FieldType::Bstring)
                        && let Some(value) =
                            crate::arrow_value::binary(batch.column(indices[0]).as_ref(), row)
                    {
                        f.encode_binary_into(value, &mut bytes)?;
                        place_field(&mut body, f.offset, &bytes);
                        return Ok(());
                    }
                    if f.kind.decimal() && f.kind != FieldType::Decimal || f.scaled_integer() {
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
            if let Some(lines) = &self.layout.lines {
                lines
                    .write(writer, &body)
                    .map_err(|e| e.context(format!("output {} record {}", self.id, *rows + 1)))?;
            } else if self.format == Format::Rdw {
                rdw.as_deref_mut()
                    .expect("RDW writer")
                    .write_record(writer, &body)
                    .map_err(|e| e.context(format!("output {} record {}", self.id, *rows + 1)))?;
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
