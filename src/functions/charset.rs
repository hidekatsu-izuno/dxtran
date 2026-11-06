//! Explicit conversions between SQL text and bytes in a named character set.
use crate::encoding::{TextEncoding, encoded_space, pad_encoded};
use datafusion::{
    arrow::{
        array::{ArrayRef, BinaryBuilder, StringBuilder},
        datatypes::DataType,
    },
    common::{Result, ScalarValue, exec_datafusion_err, plan_datafusion_err},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

pub(super) fn register(context: &SessionContext) {
    for encode in [false, true] {
        context.register_udf(ScalarUDF::from(CharsetFunction {
            encode,
            signature: Signature::variadic_any(Volatility::Immutable),
        }));
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CharsetFunction {
    encode: bool,
    signature: Signature,
}

pub(super) fn binary_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Null
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

impl ScalarUDFImpl for CharsetFunction {
    fn name(&self) -> &str {
        if self.encode {
            "icu_encode"
        } else {
            "icu_decode"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if !(2..=4).contains(&types.len()) {
            return Err(plan_datafusion_err!(
                "{} requires value, encoding, optional byte length and space_char",
                self.name()
            ));
        }
        let value_type = if self.encode {
            super::string_type(&types[0])
        } else {
            binary_type(&types[0])
        };
        if !value_type
            || !super::string_type(&types[1])
            || types
                .get(2)
                .is_some_and(|t| !t.is_integer() && *t != DataType::Null)
            || types.get(3).is_some_and(|t| !super::string_type(t))
        {
            return Err(plan_datafusion_err!(
                "{} requires {}, a character encoding, optional integer byte length and string space_char",
                self.name(),
                if self.encode { "text" } else { "binary" }
            ));
        }
        Ok(if self.encode {
            DataType::Binary
        } else {
            DataType::Utf8
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.return_type(
            &args
                .args
                .iter()
                .map(ColumnarValue::data_type)
                .collect::<Vec<_>>(),
        )?;
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let mut texts = StringBuilder::new();
        let mut binaries = BinaryBuilder::new();
        let mut encodings = HashMap::new();
        for row in 0..arrays[0].len() {
            if arrays
                .iter()
                .any(|a| a.data_type() == &DataType::Null || a.is_null(row))
            {
                if self.encode {
                    binaries.append_null();
                } else {
                    texts.append_null();
                }
                continue;
            }
            let encoding = super::string(&arrays[1], row)?;
            let space = if arrays.len() == 4 {
                super::string(&arrays[3], row)?
            } else {
                " ".into()
            };
            let key = (encoding, space);
            let width = arrays.get(2).map(|a| byte_length(a, row)).transpose()?;
            if encodings.len() == 128 && !encodings.contains_key(&key) {
                encodings.clear();
            }
            let (encoding, space) = match encodings.entry(key) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let encoding = TextEncoding::resolve(&entry.key().0)
                        .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?;
                    let space = if width.is_some() {
                        encoded_space(encoding, &entry.key().1)
                            .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?
                    } else {
                        Vec::new()
                    };
                    entry.insert((encoding, space))
                }
            };
            let bytes = if self.encode {
                encoding
                    .encode(&super::string(&arrays[0], row)?)
                    .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?
            } else {
                match ScalarValue::try_from_array(&arrays[0], row)? {
                    ScalarValue::Binary(Some(b))
                    | ScalarValue::LargeBinary(Some(b))
                    | ScalarValue::BinaryView(Some(b))
                    | ScalarValue::FixedSizeBinary(_, Some(b)) => b,
                    _ => return Err(exec_datafusion_err!("icu_decode requires binary")),
                }
            };
            if bytes.len() > crate::framing::MAX_RECORD {
                return Err(exec_datafusion_err!(
                    "{} value exceeds 16 MiB byte limit",
                    self.name()
                ));
            }
            let bytes = if let Some(width) = width {
                pad_encoded(&bytes, width, space, *encoding, false)
                    .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?
            } else {
                bytes
            };
            if self.encode {
                binaries.append_value(bytes);
            } else {
                let text = encoding
                    .decode(&bytes)
                    .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?;
                texts.append_value(text);
            }
        }
        let output: ArrayRef = if self.encode {
            Arc::new(binaries.finish())
        } else {
            Arc::new(texts.finish())
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

fn byte_length(array: &ArrayRef, row: usize) -> Result<usize> {
    let value = ScalarValue::try_from_array(array, row)?;
    let length = match value {
        ScalarValue::Int8(Some(n)) => i128::from(n),
        ScalarValue::Int16(Some(n)) => i128::from(n),
        ScalarValue::Int32(Some(n)) => i128::from(n),
        ScalarValue::Int64(Some(n)) => i128::from(n),
        ScalarValue::UInt8(Some(n)) => i128::from(n),
        ScalarValue::UInt16(Some(n)) => i128::from(n),
        ScalarValue::UInt32(Some(n)) => i128::from(n),
        ScalarValue::UInt64(Some(n)) => i128::from(n),
        _ => {
            return Err(exec_datafusion_err!(
                "encoding byte length must be an integer"
            ));
        }
    };
    usize::try_from(length)
        .ok()
        .filter(|n| *n <= crate::framing::MAX_RECORD)
        .ok_or_else(|| exec_datafusion_err!("encoding byte length must be between 0 and 16 MiB"))
}
