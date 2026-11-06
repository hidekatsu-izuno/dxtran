//! Borrow variable-width Arrow values without constructing owned scalars.
use datafusion::arrow::{
    array::{Array, AsArray},
    datatypes::DataType,
};

pub(crate) fn text(array: &dyn Array, row: usize) -> Option<&str> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(row)),
        DataType::Utf8View => Some(array.as_string_view().value(row)),
        _ => None,
    }
}

pub(crate) fn binary(array: &dyn Array, row: usize) -> Option<&[u8]> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Binary => Some(array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => Some(array.as_binary::<i64>().value(row)),
        DataType::BinaryView => Some(array.as_binary_view().value(row)),
        DataType::FixedSizeBinary(_) => Some(array.as_fixed_size_binary().value(row)),
        _ => None,
    }
}
