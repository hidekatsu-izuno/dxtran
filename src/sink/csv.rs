//! Stateless CSV formatting; character encoding remains on the ordered writer.
use super::*;
use crate::parallel::check_cancelled;
use std::sync::atomic::AtomicBool;

pub(crate) struct CsvEncoder {
    pub(super) id: String,
    pub(super) layout: RecordLayout,
    pub(super) indices: Vec<Vec<usize>>,
    pub(super) delimiter: char,
    pub(super) quote: char,
    pub(super) line_ending: &'static str,
    pub(super) quote_always: Vec<bool>,
}
impl CsvEncoder {
    // Limit buffering between records. A single large row has the same
    // allocation requirements as the serial writer; it is never split mid-cell.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn encode(
        &self,
        batch: &RecordBatch,
        offset: usize,
        start_row: u64,
        buffer: &mut String,
        limit: usize,
        cancelled: Option<&AtomicBool>,
    ) -> Result<usize> {
        let mut text = String::new();
        for row in offset..batch.num_rows() {
            if row % 256 == 0
                && let Some(cancelled) = cancelled
            {
                check_cancelled(cancelled)?;
            }
            for (field_no, (f, indices)) in self.layout.fields.iter().zip(&self.indices).enumerate()
            {
                if field_no > 0 {
                    buffer.push(self.delimiter);
                }
                let result = (|| -> Result<()> {
                    let column = batch.column(indices[0]);
                    if column.is_null(row) {
                        if f.spec.nullable != Some(true) {
                            return Err(Error::output("NULL in non-nullable CSV output"));
                        }
                        return Ok(());
                    }
                    text.clear();
                    let value = match column.data_type() {
                        DataType::Utf8 => column.as_string::<i32>().value(row),
                        DataType::LargeUtf8 => column.as_string::<i64>().value(row),
                        DataType::Utf8View => column.as_string_view().value(row),
                        DataType::Decimal128(_, scale) => {
                            let value = column.as_primitive::<Decimal128Type>().value(row);
                            let value =
                                crate::field::rescale(value, *scale as i32, f.scale as i32)?;
                            crate::field::check_precision(value, f.precision)?;
                            crate::field::decimal_text_into(value, f.scale, &mut text);
                            &text
                        }
                        _ => {
                            let value =
                                ScalarValue::try_from_array(column, row).map_err(Error::from)?;
                            text = f.format_text(&value)?;
                            &text
                        }
                    };
                    append_csv_cell(
                        buffer,
                        f.trim_output_text(value),
                        self.quote_always[field_no],
                        self.delimiter,
                        self.quote,
                    );
                    Ok(())
                })();
                result.map_err(|e| {
                    e.context(format!(
                        "output {} record {} column {} expected {:?} width {:?}",
                        self.id,
                        start_row + row as u64 + 1,
                        f.spec.name.as_deref().unwrap_or("FILLER"),
                        f.kind,
                        f.length
                    ))
                })?;
            }
            buffer.push_str(self.line_ending);
            if buffer.len() >= limit {
                return Ok(row + 1);
            }
        }
        Ok(batch.num_rows())
    }
}
