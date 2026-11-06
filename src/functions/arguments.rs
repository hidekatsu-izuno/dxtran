//! Shared scalar/array access and bounded, invocation-local format caches.
use datafusion::{
    arrow::array::ArrayRef,
    common::{Result, ScalarValue, exec_datafusion_err},
    logical_expr::ColumnarValue,
};
use std::{
    collections::{HashMap, hash_map::Entry},
    hash::Hash,
};

pub(super) struct Arguments<'a> {
    values: &'a [ColumnarValue],
    rows: usize,
    scalar: bool,
}

impl<'a> Arguments<'a> {
    pub(super) fn new(values: &'a [ColumnarValue]) -> Result<Self> {
        let mut length = None;
        for value in values {
            if let ColumnarValue::Array(array) = value {
                if length.is_some_and(|n| n != array.len()) {
                    return Err(exec_datafusion_err!("Arguments have mixed array lengths"));
                }
                length = Some(array.len());
            }
        }
        Ok(Self {
            values,
            rows: length.unwrap_or(1),
            scalar: length.is_none(),
        })
    }
    pub(super) fn len(&self) -> usize {
        self.values.len()
    }
    pub(super) fn rows(&self) -> usize {
        self.rows
    }
    pub(super) fn is_null(&self, row: usize) -> bool {
        self.values.iter().any(|value| match value {
            ColumnarValue::Scalar(value) => value.is_null(),
            ColumnarValue::Array(array) => array.is_null(row),
        })
    }
    pub(super) fn value(&self, index: usize, row: usize) -> Result<ScalarValue> {
        match &self.values[index] {
            ColumnarValue::Scalar(value) => Ok(value.clone()),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(array, row),
        }
    }
    pub(super) fn text(&self, index: usize, row: usize) -> Result<&'a str> {
        let value = match &self.values[index] {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)),
            ) => Some(s.as_str()),
            ColumnarValue::Array(array) => crate::arrow_value::text(array.as_ref(), row),
            _ => None,
        };
        value.ok_or_else(|| exec_datafusion_err!("expected non-null text"))
    }
    pub(super) fn binary(&self, index: usize, row: usize) -> Result<&'a [u8]> {
        let value = match &self.values[index] {
            ColumnarValue::Scalar(
                ScalarValue::Binary(Some(b))
                | ScalarValue::LargeBinary(Some(b))
                | ScalarValue::BinaryView(Some(b))
                | ScalarValue::FixedSizeBinary(_, Some(b)),
            ) => Some(b.as_slice()),
            ColumnarValue::Array(array) => crate::arrow_value::binary(array.as_ref(), row),
            _ => None,
        };
        value.ok_or_else(|| exec_datafusion_err!("expected non-null binary"))
    }
    pub(super) fn integer(&self, index: usize, row: usize) -> Result<i128> {
        Ok(match self.value(index, row)? {
            ScalarValue::Int8(Some(n)) => i128::from(n),
            ScalarValue::Int16(Some(n)) => i128::from(n),
            ScalarValue::Int32(Some(n)) => i128::from(n),
            ScalarValue::Int64(Some(n)) => i128::from(n),
            ScalarValue::UInt8(Some(n)) => i128::from(n),
            ScalarValue::UInt16(Some(n)) => i128::from(n),
            ScalarValue::UInt32(Some(n)) => i128::from(n),
            ScalarValue::UInt64(Some(n)) => i128::from(n),
            _ => return Err(exec_datafusion_err!("expected integer")),
        })
    }
    pub(super) fn bytes(
        &self,
        index: usize,
        row: usize,
        encoding: crate::encoding::TextEncoding,
    ) -> Result<std::borrow::Cow<'a, [u8]>> {
        if super::charset::binary_type(&self.values[index].data_type()) {
            return Ok(std::borrow::Cow::Borrowed(self.binary(index, row)?));
        }
        let text = self.text(index, row)?;
        if encoding.is_utf8() {
            Ok(std::borrow::Cow::Borrowed(text.as_bytes()))
        } else {
            encoding
                .encode(text)
                .map(std::borrow::Cow::Owned)
                .map_err(|e| exec_datafusion_err!("{e}"))
        }
    }
    pub(super) fn finish(&self, array: ArrayRef) -> Result<ColumnarValue> {
        if self.scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &array, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(array))
        }
    }
}

pub(super) struct Cache<K, V>(HashMap<K, V>);
impl<K, V> Default for Cache<K, V> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}
impl<K: Eq + Hash, V> Cache<K, V> {
    pub(super) fn get(&mut self, key: K, compile: impl FnOnce(&K) -> Result<V>) -> Result<&V> {
        if self.0.len() == 128 && !self.0.contains_key(&key) {
            self.0.clear();
        }
        Ok(match self.0.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let value = compile(entry.key())?;
                entry.insert(value)
            }
        })
    }
}
