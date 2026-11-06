//! Explicit conversions between SQL text and bytes in a named character set.
use super::arguments::{Arguments, Cache};
use crate::encoding::{TextEncoding, encoded_space, pad_encoded};
use datafusion::{
    arrow::{
        array::{ArrayRef, BinaryBuilder, StringBuilder},
        datatypes::DataType,
    },
    common::{Result, exec_datafusion_err, plan_datafusion_err},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};
use std::{borrow::Cow, sync::Arc};

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
        let arguments = Arguments::new(&args.args)?;
        let mut texts = StringBuilder::new();
        let mut binaries = BinaryBuilder::new();
        let mut encodings = Cache::default();
        for row in 0..arguments.rows() {
            if arguments.is_null(row) {
                if self.encode {
                    binaries.append_null();
                } else {
                    texts.append_null();
                }
                continue;
            }
            let encoding = arguments.text(1, row)?;
            let space = if arguments.len() == 4 {
                arguments.text(3, row)?
            } else {
                " "
            };
            let key = (encoding, space);
            let width = (arguments.len() >= 3)
                .then(|| byte_length(&arguments, row))
                .transpose()?;
            let (encoding, space) = encodings.get(key, |key| {
                let encoding = TextEncoding::resolve(key.0)
                    .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?;
                let space = if width.is_some() {
                    encoded_space(encoding, key.1)
                        .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?
                } else {
                    Vec::new()
                };
                Ok((encoding, space))
            })?;
            let bytes = if self.encode {
                Cow::Owned(
                    encoding
                        .encode(arguments.text(0, row)?)
                        .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?,
                )
            } else {
                Cow::Borrowed(arguments.binary(0, row)?)
            };
            if bytes.len() > crate::framing::MAX_RECORD {
                return Err(exec_datafusion_err!(
                    "{} value exceeds 16 MiB byte limit",
                    self.name()
                ));
            }
            let bytes = if let Some(width) = width {
                Cow::Owned(
                    pad_encoded(&bytes, width, space, *encoding, false)
                        .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?,
                )
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
        arguments.finish(output)
    }
}

fn byte_length(arguments: &Arguments<'_>, row: usize) -> Result<usize> {
    let length = arguments.integer(2, row)?;
    usize::try_from(length)
        .ok()
        .filter(|n| *n <= crate::framing::MAX_RECORD)
        .ok_or_else(|| exec_datafusion_err!("encoding byte length must be between 0 and 16 MiB"))
}
