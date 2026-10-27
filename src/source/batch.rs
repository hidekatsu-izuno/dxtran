//! Build Arrow columns and account for decoded record sizes.
use crate::{
    error::{Error, Result},
    field::{FieldType, ValueSink},
};
use datafusion::{
    arrow::{
        array::{ArrayRef, BinaryBuilder, Decimal128Builder, StringBuilder},
        datatypes::DataType,
    },
    common::ScalarValue,
    error::{DataFusionError, Result as DFResult},
};
use std::sync::Arc;

// Build common input columns directly; keep the general ScalarValue conversion
// for the remaining types. This avoids retaining a heap-allocated scalar per cell.
pub(super) enum BatchColumn {
    Text(StringBuilder),
    Decimal(Decimal128Builder),
    Binary(BinaryBuilder),
    Other(Vec<ScalarValue>),
}
impl BatchColumn {
    pub(super) fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Utf8 => Self::Text(StringBuilder::new()),
            DataType::Decimal128(_, _) => {
                Self::Decimal(Decimal128Builder::new().with_data_type(data_type.clone()))
            }
            DataType::Binary => Self::Binary(BinaryBuilder::new()),
            _ => Self::Other(Vec::new()),
        }
    }
    pub(super) fn push(&mut self, value: ScalarValue) -> DFResult<()> {
        match (self, value) {
            (Self::Text(builder), ScalarValue::Utf8(value)) => builder.append_option(value),
            (Self::Decimal(builder), ScalarValue::Decimal128(value, _, _)) => {
                builder.append_option(value)
            }
            (Self::Binary(builder), ScalarValue::Binary(value)) => builder.append_option(value),
            (Self::Other(values), value) => values.push(value),
            _ => {
                return Err(DataFusionError::Internal(
                    "input value disagrees with its column type".into(),
                ));
            }
        }
        Ok(())
    }
    pub(super) fn finish(self) -> DFResult<ArrayRef> {
        match self {
            Self::Text(mut builder) => Ok(Arc::new(builder.finish())),
            Self::Decimal(mut builder) => Ok(Arc::new(builder.finish())),
            Self::Binary(mut builder) => Ok(Arc::new(builder.finish())),
            Self::Other(values) => ScalarValue::iter_to_array(values),
        }
    }
}

pub(super) struct BatchRow<'a> {
    pub(super) columns: &'a mut [BatchColumn],
    pub(super) column: usize,
    pub(super) bytes: usize,
}

#[derive(Default)]
pub(super) struct RowSize(pub(super) usize);
impl ValueSink for RowSize {
    fn text(&mut self, value: Option<&str>) -> Result<()> {
        self.0 += size_of::<ScalarValue>() + value.map_or(0, str::len);
        Ok(())
    }
    fn decimal(&mut self, _: Option<i128>, _: u8, _: i8) -> Result<()> {
        self.0 += size_of::<ScalarValue>();
        Ok(())
    }
    fn binary(&mut self, value: Option<&[u8]>) -> Result<()> {
        self.0 += size_of::<ScalarValue>() + value.map_or(0, <[u8]>::len);
        Ok(())
    }
    fn scalar(&mut self, value: ScalarValue) -> Result<()> {
        self.0 += value.size();
        Ok(())
    }
}
impl ValueSink for BatchRow<'_> {
    fn text(&mut self, value: Option<&str>) -> Result<()> {
        let BatchColumn::Text(builder) = &mut self.columns[self.column] else {
            return Err(Error::sql("input string disagrees with its column type"));
        };
        builder.append_option(value);
        self.column += 1;
        self.bytes += size_of::<ScalarValue>() + value.map_or(0, str::len);
        Ok(())
    }
    fn decimal(&mut self, value: Option<i128>, _: u8, _: i8) -> Result<()> {
        let BatchColumn::Decimal(builder) = &mut self.columns[self.column] else {
            return Err(Error::sql("input decimal disagrees with its column type"));
        };
        builder.append_option(value);
        self.column += 1;
        self.bytes += size_of::<ScalarValue>();
        Ok(())
    }
    fn binary(&mut self, value: Option<&[u8]>) -> Result<()> {
        let BatchColumn::Binary(builder) = &mut self.columns[self.column] else {
            return Err(Error::sql("input binary disagrees with its column type"));
        };
        builder.append_option(value);
        self.column += 1;
        self.bytes += size_of::<ScalarValue>() + value.map_or(0, <[u8]>::len);
        Ok(())
    }
    fn scalar(&mut self, value: ScalarValue) -> Result<()> {
        self.bytes += value.size();
        self.columns[self.column].push(value)?;
        self.column += 1;
        Ok(())
    }
}

pub(super) fn order_key(dataset: usize, record: u64) -> [u8; 32] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut key = [0; 32];
    for (number, part) in [dataset as u64, record]
        .into_iter()
        .zip(key.as_chunks_mut::<16>().0)
    {
        for (i, byte) in part.iter_mut().enumerate() {
            *byte = HEX[((number >> (60 - i * 4)) & 15) as usize];
        }
    }
    key
}

pub(super) fn field_extra_bound(field: &crate::field::CompiledField, width: usize) -> usize {
    match field.kind {
        FieldType::String => field.encoding.max_utf8_length(width),
        FieldType::Binary => width,
        _ => 0,
    }
}
