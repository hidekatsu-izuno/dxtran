//! SQL functions for COBOL numeric editing and ICU4X Unicode datetimes.
mod cobol;

use crate::{
    config::Field as FieldConfig,
    datetime::{
        Pattern, Token,
        icu::{CalendarLocale, Formatter},
        tokenize,
    },
};
use chrono::{Datelike, FixedOffset, NaiveDate, NaiveDateTime, Offset, TimeZone};
use chrono_tz::Tz;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, new_empty_array},
        datatypes::{DataType, TimeUnit},
    },
    common::{Result, ScalarValue, exec_datafusion_err, plan_datafusion_err},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};
use std::collections::HashMap;

/// Register dxtran's SQL functions in a DataFusion session.
pub fn register(context: &SessionContext) {
    cobol::register(context);
    for kind in [Kind::Char, Kind::Date, Kind::Timestamp, Kind::Time] {
        context.register_udf(ScalarUDF::from(IcuFunction {
            kind,
            signature: Signature::variadic_any(Volatility::Immutable),
        }));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Char,
    Date,
    Timestamp,
    Time,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Char => "icu_to_char",
            Self::Date => "icu_to_date",
            Self::Timestamp => "icu_to_timestamp",
            Self::Time => "icu_to_time",
        }
    }
    fn data_type(self) -> DataType {
        match self {
            Self::Char => DataType::Utf8,
            Self::Date => DataType::Date32,
            Self::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Time => DataType::Time64(TimeUnit::Microsecond),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IcuFunction {
    kind: Kind,
    signature: Signature,
}

fn string_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
    )
}

impl ScalarUDFImpl for IcuFunction {
    fn name(&self) -> &str {
        self.kind.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        let max = if matches!(self.kind, Kind::Char | Kind::Timestamp) {
            4
        } else {
            3
        };
        if !(2..=max).contains(&types.len()) {
            return Err(plan_datafusion_err!(
                "{} requires value, pattern, optional locale{}",
                self.name(),
                if max == 4 { " and timezone" } else { "" }
            ));
        }
        if types[1..].iter().any(|t| !string_type(t)) {
            return Err(plan_datafusion_err!(
                "{} pattern, locale and timezone must be strings",
                self.name()
            ));
        }
        let valid = if self.kind == Kind::Char {
            matches!(
                types[0],
                DataType::Null
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Timestamp(_, _)
            )
        } else {
            string_type(&types[0])
        };
        if !valid {
            return Err(plan_datafusion_err!(
                "unsupported value type {} for {}",
                types[0],
                self.name()
            ));
        }
        Ok(self.kind.data_type())
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
        let rows = arrays[0].len();
        let null = ScalarValue::try_from(&self.kind.data_type())?;
        let mut cache = HashMap::new();
        let mut values = Vec::with_capacity(rows);
        for row in 0..rows {
            if arrays
                .iter()
                .any(|a| a.data_type() == &DataType::Null || a.is_null(row))
            {
                values.push(null.clone());
                continue;
            }
            let pattern = string(&arrays[1], row)?;
            let locale = if arrays.len() >= 3 {
                string(&arrays[2], row)?
            } else {
                "en-US".into()
            };
            let timezone = if arrays.len() == 4 {
                Some(string(&arrays[3], row)?)
            } else {
                None
            };
            let key = (pattern, locale, timezone);
            if !cache.contains_key(&key) {
                let locale =
                    CalendarLocale::new(&key.1).map_err(|e| exec_datafusion_err!("{e}"))?;
                let compiled = if self.kind == Kind::Char {
                    let tokens = tokenize(&key.0).map_err(|e| exec_datafusion_err!("{e}"))?;
                    let parts: Vec<char> = tokens
                        .iter()
                        .filter_map(|t| {
                            if let Token::Part(c, _) = t {
                                Some(*c)
                            } else {
                                None
                            }
                        })
                        .collect();
                    Compiled::Format {
                        formatter: Box::new(
                            Formatter::new(&key.0, &locale)
                                .map_err(|e| exec_datafusion_err!("{e}"))?,
                        ),
                        date: parts.iter().any(|c| "GyYuUrQqMLlwWdDFgEec".contains(*c)),
                        time: parts.iter().any(|c| "abBhHKkmsSA".contains(*c)),
                        offset: parts.iter().any(|c| "zZOvVXx".contains(*c)),
                    }
                } else {
                    let field = FieldConfig {
                        datetime_pattern: Some(key.0.clone()),
                        unit: Some("us".into()),
                        timezone: Some(key.2.clone().unwrap_or_else(|| "UTC".into())),
                        timezone_set: true,
                        ..Default::default()
                    };
                    let kind = match self.kind {
                        Kind::Date => "date",
                        Kind::Time => "time",
                        _ => "timestamp",
                    };
                    Compiled::Parse(Box::new(
                        Pattern::compile_icu(&field, kind, &locale)
                            .map_err(|e| exec_datafusion_err!("{e}"))?,
                    ))
                };
                cache.insert(key.clone(), compiled);
            }
            let value = match &cache[&key] {
                Compiled::Format {
                    formatter,
                    date,
                    time,
                    offset,
                } => {
                    let value = ScalarValue::try_from_array(&arrays[0], row)?;
                    let input_date =
                        matches!(value, ScalarValue::Date32(_) | ScalarValue::Date64(_));
                    let input_time = matches!(
                        value,
                        ScalarValue::Time32Second(_)
                            | ScalarValue::Time32Millisecond(_)
                            | ScalarValue::Time64Microsecond(_)
                            | ScalarValue::Time64Nanosecond(_)
                    );
                    if (*date && input_time) || (*time && input_date) {
                        return Err(exec_datafusion_err!(
                            "{} pattern requires fields absent from the value",
                            self.name()
                        ));
                    }
                    let local = local_datetime(&value, key.2.as_deref())?;
                    if *offset && !local.aware {
                        return Err(exec_datafusion_err!(
                            "{} timezone pattern requires a timestamp with a timezone",
                            self.name()
                        ));
                    }
                    ScalarValue::Utf8(Some(
                        formatter
                            .format(local.dt, &local.zone, local.offset)
                            .map_err(|e| exec_datafusion_err!("{e}"))?,
                    ))
                }
                Compiled::Parse(pattern) => {
                    let text = string(&arrays[0], row)?;
                    let value = pattern
                        .parse(&text)
                        .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?;
                    // Input zones determine the instant; SQL results use a stable UTC type.
                    if let ScalarValue::TimestampMicrosecond(value, _) = value {
                        ScalarValue::TimestampMicrosecond(value, Some("UTC".into()))
                    } else {
                        value
                    }
                }
            };
            values.push(value);
        }
        let array = if rows == 0 {
            new_empty_array(&self.kind.data_type())
        } else {
            ScalarValue::iter_to_array(values)?
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &array, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(array))
        }
    }
}

enum Compiled {
    Format {
        formatter: Box<Formatter>,
        date: bool,
        time: bool,
        offset: bool,
    },
    Parse(Box<Pattern>),
}

fn string(array: &ArrayRef, row: usize) -> Result<String> {
    match ScalarValue::try_from_array(array, row)? {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Ok(s),
        _ => Err(exec_datafusion_err!(
            "ICU datetime argument must be a non-null string"
        )),
    }
}

struct LocalDateTime {
    dt: NaiveDateTime,
    zone: String,
    offset: i32,
    aware: bool,
}

fn local_datetime(value: &ScalarValue, override_zone: Option<&str>) -> Result<LocalDateTime> {
    let bad = || exec_datafusion_err!("ICU datetime value out of range");
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let (dt, metadata) = match value {
        ScalarValue::Date32(Some(d)) => (
            epoch
                .checked_add_signed(chrono::Duration::days(i64::from(*d)))
                .ok_or_else(bad)?,
            None,
        ),
        ScalarValue::Date64(Some(t)) => (
            chrono::DateTime::from_timestamp_millis(*t)
                .ok_or_else(bad)?
                .naive_utc(),
            None,
        ),
        ScalarValue::Time32Second(Some(t)) => (time_of_day(epoch, i64::from(*t), 1)?, None),
        ScalarValue::Time32Millisecond(Some(t)) => (time_of_day(epoch, i64::from(*t), 1000)?, None),
        ScalarValue::Time64Microsecond(Some(t)) => (time_of_day(epoch, *t, 1_000_000)?, None),
        ScalarValue::Time64Nanosecond(Some(t)) => (time_of_day(epoch, *t, 1_000_000_000)?, None),
        _ => {
            let (t, scale, zone) = match value {
                ScalarValue::TimestampSecond(Some(t), z) => (*t, 1, z),
                ScalarValue::TimestampMillisecond(Some(t), z) => (*t, 1000, z),
                ScalarValue::TimestampMicrosecond(Some(t), z) => (*t, 1_000_000, z),
                ScalarValue::TimestampNanosecond(Some(t), z) => (*t, 1_000_000_000, z),
                _ => return Err(bad()),
            };
            (
                chrono::DateTime::from_timestamp(
                    t.div_euclid(scale),
                    (t.rem_euclid(scale) * (1_000_000_000 / scale)) as u32,
                )
                .ok_or_else(bad)?
                .naive_utc(),
                zone.as_deref(),
            )
        }
    };
    // Reject dates outside ICU's range before applying an offset, which could otherwise
    // overflow Chrono's local datetime near its much wider calendar limits.
    if !(-9999..=9999).contains(&dt.year()) {
        return Err(bad());
    }
    let Some(zone) = override_zone.or(metadata) else {
        return Ok(LocalDateTime {
            dt,
            zone: "UTC".into(),
            offset: 0,
            aware: false,
        });
    };
    if !matches!(
        value,
        ScalarValue::TimestampSecond(_, _)
            | ScalarValue::TimestampMillisecond(_, _)
            | ScalarValue::TimestampMicrosecond(_, _)
            | ScalarValue::TimestampNanosecond(_, _)
    ) {
        return Err(exec_datafusion_err!(
            "ICU datetime timezone is only valid for timestamps"
        ));
    }
    let (dt, offset) = if let Ok(zone) = zone.parse::<Tz>() {
        let zoned = if metadata.is_some() {
            zone.from_utc_datetime(&dt)
        } else {
            zone.from_local_datetime(&dt)
                .single()
                .ok_or_else(|| exec_datafusion_err!("nonexistent or ambiguous local timestamp"))?
        };
        (zoned.naive_local(), zoned.offset().fix().local_minus_utc())
    } else if let Ok(zone) = zone.parse::<FixedOffset>() {
        let zoned = if metadata.is_some() {
            zone.from_utc_datetime(&dt)
        } else {
            zone.from_local_datetime(&dt).single().ok_or_else(bad)?
        };
        (zoned.naive_local(), zone.local_minus_utc())
    } else {
        return Err(exec_datafusion_err!("unknown ICU datetime timezone"));
    };
    Ok(LocalDateTime {
        dt,
        zone: zone.into(),
        offset,
        aware: true,
    })
}

fn time_of_day(epoch: NaiveDateTime, value: i64, scale: i64) -> Result<NaiveDateTime> {
    if !(0..86_400 * scale).contains(&value) {
        return Err(exec_datafusion_err!("ICU time value out of range"));
    }
    epoch
        .checked_add_signed(chrono::Duration::nanoseconds(
            value * (1_000_000_000 / scale),
        ))
        .ok_or_else(|| exec_datafusion_err!("ICU time value out of range"))
}
