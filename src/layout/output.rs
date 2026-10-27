//! SQL-to-output compatibility checks.
use super::RecordLayout;
use crate::{
    config::{Dataset, Format},
    error::{Error, Result},
    field::{CompiledField, FieldType},
};
use datafusion::arrow::datatypes::{DataType, Schema};
use std::collections::BTreeSet;

impl RecordLayout {
    pub(super) fn validate_output(&self, d: &Dataset, schema: &Schema) -> Result<()> {
        let mut names = BTreeSet::new();
        for f in schema.fields() {
            if !crate::config::identifier(f.name()) || !names.insert(f.name()) {
                return Err(Error::config(
                    "SQL output column names must be unique public identifiers",
                ));
            }
        }
        let physical: BTreeSet<_> = self.fields.iter().flat_map(CompiledField::names).collect();
        if d.format != Format::Csv && physical != names.iter().map(|s| (*s).clone()).collect() {
            return Err(Error::config(
                "physical output fields must match the SQL column name set exactly",
            ));
        }
        for f in &self.fields {
            for name in f.names() {
                let col = schema
                    .field_with_name(&name)
                    .map_err(|e| Error::config(e.to_string()))?;
                let explicit = d
                    .fields
                    .iter()
                    .find(|s| s.name.as_deref() == Some(&name))
                    .is_some_and(|s| s.kind.is_some());
                if let DataType::Timestamp(_, zone) = col.data_type()
                    && matches!(f.kind, FieldType::Timestamp)
                    && zone.is_some() != matches!(f.data_type(), DataType::Timestamp(_, Some(_)))
                {
                    return Err(Error::config(
                        "timestamp timezone presence requires explicit SQL conversion",
                    ));
                }
                if d.format == Format::Csv && !explicit {
                    if !csv_type(col.data_type()) {
                        return Err(Error::config(format!(
                            "unsupported CSV SQL type {:?}",
                            col.data_type()
                        )));
                    }
                    continue;
                }
                let t = col.data_type();
                use FieldType::*;
                let compatible = match f.kind {
                    String => {
                        matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
                    }
                    Decimal | Zoned | Lzoned | Tzoned | Uzoned | Packed | Upacked | Npacked => {
                        matches!(t, DataType::Decimal128(_, _))
                    }
                    Boolean | Bits => *t == DataType::Boolean,
                    Date => *t == DataType::Date32,
                    Time => matches!(t, DataType::Time64(_)),
                    Timestamp => {
                        matches!(t,DataType::Timestamp(_,z) if z.is_some()==matches!(f.data_type(),DataType::Timestamp(_,Some(_))))
                    }
                    Binary => matches!(
                        t,
                        DataType::Binary
                            | DataType::BinaryView
                            | DataType::LargeBinary
                            | DataType::FixedSizeBinary(_)
                    ),
                    IntLe | IntBe | UintLe | UintBe => t.is_integer(),
                    FloatLe | FloatBe | HfloatLe | HfloatBe => {
                        matches!(t, DataType::Float32 | DataType::Float64)
                    }
                    Filler => true,
                };
                if !compatible {
                    return Err(Error::config(format!(
                        "output column '{name}' type {t:?} is incompatible with {:?}; use SQL CAST",
                        f.kind
                    )));
                }
            }
        }
        Ok(())
    }
}
fn csv_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Date32
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Binary
            | DataType::BinaryView
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            | DataType::Decimal128(_, _)
            | DataType::Float32
            | DataType::Float64
    ) || t.is_integer()
}
