//! Resolve CSV output field overrides against the SQL schema.
use crate::{
    config::{Dataset, Field},
    error::{Error, Result},
};
use datafusion::arrow::datatypes::{DataType, Schema, TimeUnit};
use std::collections::BTreeSet;

pub(super) fn output_fields(d: &Dataset, schema: &Schema) -> Result<Vec<Field>> {
    let specs = &d.fields;
    let mut names = BTreeSet::new();
    for f in specs {
        let name = f
            .name
            .as_deref()
            .ok_or_else(|| Error::config("CSV output field name required"))?;
        if !names.insert(name) || schema.index_of(name).is_err() {
            return Err(Error::config(format!(
                "unknown or duplicate CSV output column '{name}'"
            )));
        }
    }
    let specs = schema
        .fields()
        .iter()
        .map(|column| {
            let mut f = specs
                .iter()
                .find(|f| f.name.as_deref() == Some(column.name()))
                .cloned()
                .unwrap_or_else(|| Field {
                    name: Some(column.name().clone()),
                    ..Default::default()
                });
            let inferred = match column.data_type() {
                DataType::Boolean => "boolean",
                DataType::Date32 => "date",
                DataType::Time64(_) => "time",
                DataType::Timestamp(_, _) => "timestamp",
                DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_) => "binary",
                DataType::Decimal128(_, _) => "decimal",
                _ => "string",
            };
            if f.kind.is_none() {
                f.kind = Some(inferred.into());
            }
            if let DataType::Decimal128(p, s) = column.data_type()
                && *s >= 0
            {
                f.precision.get_or_insert(*p);
                f.scale.get_or_insert(*s as u8);
            }
            if let DataType::Time64(u) | DataType::Timestamp(u, _) = column.data_type()
                && f.unit.is_none()
            {
                f.unit = Some(
                    match u {
                        TimeUnit::Second => "s",
                        TimeUnit::Millisecond => "ms",
                        TimeUnit::Microsecond => "us",
                        TimeUnit::Nanosecond => "ns",
                    }
                    .into(),
                );
            }
            if let DataType::Timestamp(_, zone) = column.data_type()
                && !f.timezone_set
            {
                f.timezone = zone.as_ref().map(ToString::to_string);
                f.timezone_set = true;
            }
            f.nullable.get_or_insert(column.is_nullable());
            f
        })
        .collect();
    Ok(specs)
}
