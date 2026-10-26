use crate::{
    config::Field,
    error::{Error, Result},
};
use chrono::{Datelike, FixedOffset, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use datafusion::{
    arrow::datatypes::{DataType, TimeUnit},
    common::ScalarValue,
};
use regex::Regex;

#[derive(Clone, Debug)]
enum Token {
    Literal(String),
    Part(char, usize),
}
#[derive(Clone, Debug)]
pub struct Pattern {
    tokens: Vec<Token>,
    regex: Regex,
    kind: String,
    unit: TimeUnit,
    zone: Option<Tz>,
    year_start: i32,
}
pub fn unit(s: Option<&str>) -> Result<TimeUnit> {
    match s.unwrap_or("us") {
        "s" => Ok(TimeUnit::Second),
        "ms" => Ok(TimeUnit::Millisecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ns" => Ok(TimeUnit::Nanosecond),
        _ => Err(Error::config("invalid datetime unit")),
    }
}
fn divisor(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1000,
        TimeUnit::Nanosecond => 1,
    }
}
impl Pattern {
    pub fn compile(field: &Field, kind: &str) -> Result<Self> {
        let unit = unit(field.unit.as_deref())?;
        if kind == "time" && !matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) {
            return Err(Error::config("time unit must be us or ns"));
        }
        let zone = if field.timezone_set {
            field.timezone.as_deref()
        } else {
            Some("UTC")
        }
        .map(|s| {
            s.parse::<Tz>()
                .map_err(|_| Error::config("unknown IANA timezone"))
        })
        .transpose()?;
        let frac = match unit {
            TimeUnit::Second => "",
            TimeUnit::Millisecond => ".SSS",
            TimeUnit::Microsecond => ".SSSSSS",
            TimeUnit::Nanosecond => ".SSSSSSSSS",
        };
        let default = match kind {
            "date" => "yyyy-MM-dd".to_owned(),
            "time" => format!("HH:mm:ss{frac}"),
            _ => format!(
                "yyyy-MM-dd'T'HH:mm:ss{frac}{}",
                if zone.is_some() { "XXX" } else { "" }
            ),
        };
        let source = field.datetime_pattern.as_deref().unwrap_or(&default);
        let chars: Vec<char> = source.chars().collect();
        let mut i = 0;
        let mut tokens = vec![];
        while i < chars.len() {
            let c = chars[i];
            i += 1;
            if c == '\'' {
                let mut literal = String::new();
                if chars.get(i) == Some(&'\'') {
                    literal.push('\'');
                    i += 1;
                } else {
                    let mut closed = false;
                    while i < chars.len() {
                        let c = chars[i];
                        i += 1;
                        if c == '\'' {
                            if chars.get(i) == Some(&'\'') {
                                literal.push('\'');
                                i += 1;
                            } else {
                                closed = true;
                                break;
                            }
                        } else {
                            literal.push(c);
                        }
                    }
                    if !closed {
                        return Err(Error::config("unclosed datetime pattern literal"));
                    }
                }
                tokens.push(Token::Literal(literal));
            } else if c.is_ascii_alphabetic() {
                let start = i - 1;
                while chars.get(i) == Some(&c) {
                    i += 1;
                }
                let n = i - start;
                let valid = match c {
                    'y' | 'u' => n <= 10,
                    'M' | 'd' | 'H' | 'h' | 'm' | 's' => n <= 2,
                    'S' => n <= 9,
                    'a' => n == 1,
                    'X' | 'x' => n <= 5,
                    'Z' => n <= 3,
                    _ => false,
                };
                if !valid {
                    return Err(Error::config(format!(
                        "unsupported datetime pattern field {c}"
                    )));
                }
                tokens.push(Token::Part(c, n));
            } else {
                tokens.push(Token::Literal(c.to_string()));
            }
        }
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
        let has = |c| parts.contains(&c);
        let mut seen = std::collections::BTreeSet::new();
        for c in &parts {
            let key = match c {
                'u' => 'y',
                'h' => 'H',
                'x' | 'Z' => 'X',
                c => *c,
            };
            if !seen.insert(key) {
                return Err(Error::config("duplicate datetime pattern field"));
            }
        }
        if has('h') != has('a')
            || (kind == "date" && parts.iter().any(|c| !"yuMd".contains(*c)))
            || (kind == "time" && parts.iter().any(|c| "yuMdXxZ".contains(*c)))
        {
            return Err(Error::config("incompatible datetime pattern fields"));
        }
        if (kind != "time" && (!(has('y') || has('u')) || !has('M') || !has('d')))
            || (kind != "date" && !(has('H') || has('h')))
        {
            return Err(Error::config("datetime pattern lacks required fields"));
        }
        if kind == "timestamp" && zone.is_none() && (has('X') || has('x') || has('Z')) {
            return Err(Error::config("timezone null cannot have an offset pattern"));
        }
        let has_yy = tokens.iter().any(|t| matches!(t, Token::Part('y', 2)));
        let year_start = field.two_digit_year_start.unwrap_or(1970);
        if !(1..=9900).contains(&year_start) || (field.two_digit_year_start.is_some() && !has_yy) {
            return Err(Error::config(
                "two_digit_year_start requires yy and a year in 1..9900",
            ));
        }
        let mut regex = String::from("\\A");
        for (i, t) in tokens.iter().enumerate() {
            match t {
                Token::Literal(s) => regex.push_str(&regex::escape(s)),
                Token::Part(c, n) => {
                    let adjacent = |j: usize| matches!(tokens.get(j),Some(Token::Part(c,_)) if "yuMdHhmsS".contains(*c));
                    let numeric = "yuMdHhmsS".contains(*c);
                    let joined = numeric && ((i > 0 && adjacent(i - 1)) || adjacent(i + 1));
                    if joined && *n == 1 {
                        return Err(Error::config("ambiguous adjacent datetime numeric fields"));
                    }
                    let p = match c {
                        'a' => "AM|PM".into(),
                        'X' | 'x' | 'Z' => {
                            let off = match (*c, *n) {
                                ('Z', _) | (_, 2) => "[+-][0-9]{4}",
                                (_, 1) => "[+-][0-9]{2}(?:[0-9]{2})?",
                                (_, 3) => "[+-][0-9]{2}:[0-9]{2}",
                                (_, 4) => "[+-][0-9]{4}(?:[0-9]{2})?",
                                _ => "[+-][0-9]{2}:[0-9]{2}(?::[0-9]{2})?",
                            };
                            if *c == 'X' {
                                format!("Z|{off}")
                            } else {
                                off.into()
                            }
                        }
                        'y' | 'u' if !(*c == 'y' && *n == 2) && !joined => {
                            format!("{}[0-9]{{{},}}", if *c == 'u' { "-?" } else { "" }, n)
                        }
                        'u' if joined => format!("-?[0-9]{{{n}}}"),
                        _ if *n == 1 && *c != 'S' => "[0-9]{1,2}".into(),
                        _ => format!("[0-9]{{{n}}}"),
                    };
                    regex.push('(');
                    regex.push_str(&p);
                    regex.push(')');
                }
            }
        }
        regex.push_str("\\z");
        Ok(Self {
            tokens,
            regex: Regex::new(&regex).map_err(|e| Error::config(e.to_string()))?,
            kind: kind.into(),
            unit,
            zone,
            year_start,
        })
    }
    pub fn data_type(&self) -> DataType {
        match self.kind.as_str() {
            "date" => DataType::Date32,
            "time" => DataType::Time64(self.unit),
            _ => DataType::Timestamp(self.unit, self.zone.map(|z| z.name().into())),
        }
    }
    pub fn parse(&self, s: &str) -> Result<ScalarValue> {
        let bad = || Error::input("value does not match datetime pattern or range");
        let cap = self.regex.captures(s).ok_or_else(bad)?;
        let (mut year, mut month, mut day, mut hour, mut minute, mut second, mut nanos) =
            (1970, 1, 1, 0, 0, 0, 0);
        let (mut am, mut h12, mut offset) = (None, false, None);
        let mut idx = 1;
        for t in &self.tokens {
            if let Token::Part(c, n) = t {
                let text = &cap[idx];
                idx += 1;
                match c {
                    'a' => am = Some(text == "PM"),
                    'X' | 'x' | 'Z' => {
                        let seconds = if text == "Z" {
                            0
                        } else {
                            let digits = text[1..].replace(':', "");
                            let h: i32 = digits[..2].parse().map_err(|_| bad())?;
                            let m: i32 = if digits.len() >= 4 {
                                digits[2..4].parse().map_err(|_| bad())?
                            } else {
                                0
                            };
                            let sec: i32 = if digits.len() >= 6 {
                                digits[4..6].parse().map_err(|_| bad())?
                            } else {
                                0
                            };
                            if h > 23 || m > 59 || sec > 59 {
                                return Err(bad());
                            }
                            (h * 3600 + m * 60 + sec) * if text.starts_with('-') { -1 } else { 1 }
                        };
                        offset = Some(FixedOffset::east_opt(seconds).ok_or_else(bad)?);
                    }
                    _ => {
                        let v: i32 = text.parse().map_err(|_| bad())?;
                        match c {
                            'y' if *n == 2 => {
                                year = self.year_start
                                    + (v - self.year_start.rem_euclid(100)).rem_euclid(100)
                            }
                            'y' => {
                                if v == 0 {
                                    return Err(bad());
                                }
                                year = v
                            }
                            'u' => year = v,
                            'M' => month = v as u32,
                            'd' => day = v as u32,
                            'H' => hour = v as u32,
                            'h' => {
                                h12 = true;
                                hour = v as u32
                            }
                            'm' => minute = v as u32,
                            's' => second = v as u32,
                            'S' => nanos = v as u32 * 10u32.pow(9 - *n as u32),
                            _ => (),
                        }
                    }
                }
            }
        }
        if h12 {
            if !(1..=12).contains(&hour) {
                return Err(bad());
            }
            hour = hour % 12 + if am == Some(true) { 12 } else { 0 };
        }
        let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(bad)?;
        if self.kind == "date" {
            return Ok(ScalarValue::Date32(Some(
                (date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                    .num_days()
                    .try_into()
                    .map_err(|_| bad())?,
            )));
        }
        if second >= 60 {
            return Err(bad());
        }
        let time = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos).ok_or_else(bad)?;
        let div = divisor(&self.unit);
        if i64::from(nanos) % div != 0 {
            return Err(Error::input("datetime precision exceeds unit"));
        }
        if self.kind == "time" {
            let v = (i64::from(time.num_seconds_from_midnight()) * 1_000_000_000
                + i64::from(nanos))
                / div;
            return Ok(if self.unit == TimeUnit::Nanosecond {
                ScalarValue::Time64Nanosecond(Some(v))
            } else {
                ScalarValue::Time64Microsecond(Some(v))
            });
        }
        let local = date.and_time(time);
        let utc = if let Some(off) = offset {
            off.from_local_datetime(&local)
                .single()
                .ok_or_else(bad)?
                .with_timezone(&Utc)
        } else if let Some(zone) = self.zone {
            zone.from_local_datetime(&local)
                .single()
                .ok_or_else(|| Error::input("nonexistent or ambiguous local timestamp"))?
                .with_timezone(&Utc)
        } else {
            local.and_utc()
        };
        let v = match self.unit {
            TimeUnit::Second => utc.timestamp(),
            TimeUnit::Millisecond => utc.timestamp_millis(),
            TimeUnit::Microsecond => utc.timestamp_micros(),
            TimeUnit::Nanosecond => utc.timestamp_nanos_opt().ok_or_else(bad)?,
        };
        let zone = self.zone.map(|z| z.name().into());
        Ok(match self.unit {
            TimeUnit::Second => ScalarValue::TimestampSecond(Some(v), zone),
            TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(v), zone),
            TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(v), zone),
            TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(v), zone),
        })
    }
    pub fn format(&self, v: &ScalarValue) -> Result<String> {
        let bad = || Error::output("datetime out of range or incompatible type");
        let (dt, offset) = match v {
            ScalarValue::Date32(Some(d)) => (
                NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .checked_add_signed(chrono::Duration::days(i64::from(*d)))
                    .ok_or_else(bad)?
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                0,
            ),
            ScalarValue::Time64Microsecond(Some(t)) | ScalarValue::Time64Nanosecond(Some(t)) => {
                let factor = if matches!(v, ScalarValue::Time64Microsecond(_)) {
                    1000
                } else {
                    1
                };
                let ns = t
                    .checked_mul(factor)
                    .filter(|n| (0..86_400_000_000_000).contains(n))
                    .ok_or_else(bad)?;
                (
                    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_time(
                        NaiveTime::from_num_seconds_from_midnight_opt(
                            (ns / 1_000_000_000) as u32,
                            (ns % 1_000_000_000) as u32,
                        )
                        .unwrap(),
                    ),
                    0,
                )
            }
            _ => {
                let (t, scale, aware) = match v {
                    ScalarValue::TimestampSecond(Some(t), z) => (*t, 1, z.is_some()),
                    ScalarValue::TimestampMillisecond(Some(t), z) => (*t, 1000, z.is_some()),
                    ScalarValue::TimestampMicrosecond(Some(t), z) => (*t, 1_000_000, z.is_some()),
                    ScalarValue::TimestampNanosecond(Some(t), z) => {
                        (*t, 1_000_000_000, z.is_some())
                    }
                    _ => return Err(bad()),
                };
                if aware != self.zone.is_some() {
                    return Err(Error::output(
                        "timestamp timezone presence requires explicit SQL conversion",
                    ));
                }
                let dt = chrono::DateTime::from_timestamp(
                    t.div_euclid(scale),
                    (t.rem_euclid(scale) * (1_000_000_000 / scale)) as u32,
                )
                .ok_or_else(bad)?;
                if let Some(zone) = self.zone {
                    let z = dt.with_timezone(&zone);
                    use chrono::Offset;
                    (z.naive_local(), z.offset().fix().local_minus_utc())
                } else {
                    (dt.naive_utc(), 0)
                }
            }
        };
        if i64::from(dt.nanosecond()) % divisor(&self.unit) != 0 {
            return Err(Error::output("datetime unit would lose precision"));
        }
        let max_frac = self
            .tokens
            .iter()
            .find_map(|t| {
                if let Token::Part('S', n) = t {
                    Some(*n)
                } else {
                    None
                }
            })
            .unwrap_or(0);
        if dt.nanosecond() % 10u32.pow(9 - max_frac as u32) != 0 {
            return Err(Error::output(
                "datetime pattern would lose fractional precision",
            ));
        }
        if !self.tokens.iter().any(|t| matches!(t, Token::Part('m', _))) && dt.minute() != 0
            || !self.tokens.iter().any(|t| matches!(t, Token::Part('s', _))) && dt.second() != 0
        {
            return Err(Error::output("datetime pattern would lose time precision"));
        }
        let mut out = String::new();
        for t in &self.tokens {
            match t {
                Token::Literal(s) => out.push_str(s),
                Token::Part(c, n) => {
                    let v = match c {
                        'y' if *n == 2 => {
                            if !(self.year_start..self.year_start + 100).contains(&dt.year()) {
                                return Err(Error::output(
                                    "year outside two-digit restoration window",
                                ));
                            }
                            dt.year().rem_euclid(100)
                        }
                        'y' => {
                            if dt.year() <= 0 {
                                return Err(bad());
                            }
                            dt.year()
                        }
                        'u' => dt.year(),
                        'M' => dt.month() as i32,
                        'd' => dt.day() as i32,
                        'H' => dt.hour() as i32,
                        'h' => ((dt.hour() + 11) % 12 + 1) as i32,
                        'm' => dt.minute() as i32,
                        's' => dt.second() as i32,
                        'S' => (dt.nanosecond() / 10u32.pow(9 - *n as u32)) as i32,
                        _ => 0,
                    };
                    match c {
                        'a' => out.push_str(if dt.hour() < 12 { "AM" } else { "PM" }),
                        'X' | 'x' | 'Z' => {
                            if offset == 0 && *c == 'X' {
                                out.push('Z');
                                continue;
                            }
                            let a = offset.unsigned_abs();
                            let (h, m, s) = (a / 3600, a / 60 % 60, a % 60);
                            // Z / ZZ / ZZZ always use RFC 822's numeric HHmm form.
                            let width = if *c == 'Z' { 2 } else { *n };
                            out.push(if offset < 0 { '-' } else { '+' });
                            out.push_str(&format!("{h:02}"));
                            let colon = matches!(width, 3 | 5);
                            if width != 1 || m != 0 || s != 0 {
                                if colon {
                                    out.push(':');
                                }
                                out.push_str(&format!("{m:02}"));
                            }
                            if width >= 4 && s != 0 {
                                if colon {
                                    out.push(':');
                                }
                                out.push_str(&format!("{s:02}"));
                            } else if s != 0 {
                                return Err(Error::output("offset pattern loses seconds"));
                            }
                        }
                        _ if v < 0 => {
                            out.push_str(&format!("-{:0width$}", v.unsigned_abs(), width = *n))
                        }
                        _ => out.push_str(&format!("{v:0width$}", width = *n)),
                    }
                }
            }
        }
        Ok(out)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn f(p: &str) -> Field {
        Field {
            datetime_pattern: Some(p.into()),
            ..Default::default()
        }
    }
    #[test]
    fn unicode_patterns() {
        let p = Pattern::compile(&f("yyyyMMdd"), "date").unwrap();
        assert_eq!(p.format(&p.parse("20261025").unwrap()).unwrap(), "20261025");
        assert!(p.parse("20260230").is_err());
        assert!(Pattern::compile(&f("YYYYMMdd"), "date").is_err());
        assert!(Pattern::compile(&f("yyyyMdd"), "date").is_err());
    }
    #[test]
    fn yy() {
        let p = Pattern::compile(&f("yy-MM-dd"), "date").unwrap();
        assert_eq!(
            Pattern::compile(&f("yyyy-MM-dd"), "date")
                .unwrap()
                .format(&p.parse("69-01-01").unwrap())
                .unwrap(),
            "2069-01-01"
        );
    }
    #[test]
    fn dst() {
        let mut f = f("yyyy-MM-dd HH:mm:ss");
        f.timezone_set = true;
        f.timezone = Some("America/New_York".into());
        let p = Pattern::compile(&f, "timestamp").unwrap();
        assert!(p.parse("2026-11-01 01:30:00").is_err());
        assert!(p.parse("2026-03-08 02:30:00").is_err());
    }
    #[test]
    fn rfc822_offsets_always_include_hours_and_minutes_without_colons() {
        for suffix in ["Z", "ZZ", "ZZZ"] {
            for (zone, expected_offset) in [
                ("UTC", "+0000"),
                ("Asia/Tokyo", "+0900"),
                ("Asia/Kolkata", "+0530"),
                ("America/St_Johns", "-0330"),
            ] {
                let mut field = f(&format!("yyyy-MM-dd HH:mm:ss{suffix}"));
                field.timezone_set = true;
                field.timezone = Some(zone.into());
                let pattern = Pattern::compile(&field, "timestamp").unwrap();
                let expected = format!("2026-01-02 12:34:56{expected_offset}");
                let value = pattern.parse(&expected).unwrap();
                assert_eq!(pattern.format(&value).unwrap(), expected, "{zone} {suffix}");
            }
        }
    }
    #[test]
    fn extended_years_and_exact_fractional_precision() {
        let p = Pattern::compile(&f("uuuu-MM-dd"), "date").unwrap();
        for s in ["0000-01-01", "-0001-01-01"] {
            assert_eq!(p.format(&p.parse(s).unwrap()).unwrap(), s);
        }
        let p = Pattern::compile(&f("HH:mm:ss.SSS"), "time").unwrap();
        assert!(p.parse("12:34:56.12").is_err());
        assert!(p.parse("12:34:56.1234").is_err());
        assert_eq!(
            p.format(&p.parse("12:34:56.123").unwrap()).unwrap(),
            "12:34:56.123"
        );
        assert!(p.format(&ScalarValue::Time64Microsecond(Some(1))).is_err());
    }
}
