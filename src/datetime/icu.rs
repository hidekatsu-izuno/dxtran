//! ICU owns calendar conversion and localized names; the shared codec owns strict parsing.
use crate::error::{Error, Result};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use icu_calendar::{AnyCalendarKind, Date, Gregorian, Iso, cal::Japanese, types::YearInput};
use icu_datetime::{
    fieldsets::enums::CompositeFieldSet,
    input::{DateTime, Time, TimeZone, UtcOffset, ZonedDateTime},
    pattern::{DateTimePattern, FixedCalendarDateTimeNames},
};
use icu_locale::{Locale, extensions::unicode::key};
use writeable::TryWriteable;

fn icu_error(error: impl std::fmt::Display) -> Error {
    Error::sql(format!("ICU datetime: {error}"))
}

#[derive(Clone, Debug)]
pub(crate) struct CalendarLocale {
    locale: Locale,
    pub japanese: bool,
}

impl CalendarLocale {
    pub fn new(locale: &str) -> Result<Self> {
        let locale: Locale = locale.parse().map_err(icu_error)?;
        if let Some(calendar) = locale.extensions.unicode.keywords.get(&key!("ca"))
            && !matches!(
                calendar.to_string().as_str(),
                "gregory" | "iso8601" | "japanese"
            )
        {
            return Err(Error::sql(
                "ICU datetime supports gregory and japanese calendars",
            ));
        }
        let kind = AnyCalendarKind::try_new(locale.clone().into()).map_err(icu_error)?;
        let japanese = match kind {
            AnyCalendarKind::Japanese => true,
            AnyCalendarKind::Gregorian | AnyCalendarKind::Iso => false,
            _ => {
                return Err(Error::sql(
                    "ICU datetime supports gregory and japanese calendars",
                ));
            }
        };
        Ok(Self { locale, japanese })
    }
}

#[derive(Debug)]
enum Names {
    Gregorian(FixedCalendarDateTimeNames<Gregorian, CompositeFieldSet>),
    Japanese(FixedCalendarDateTimeNames<Japanese, CompositeFieldSet>),
}

#[derive(Debug)]
pub(crate) struct Formatter {
    pattern: DateTimePattern,
    names: Names,
}

impl Formatter {
    pub fn new(pattern: &str, locale: &CalendarLocale) -> Result<Self> {
        let pattern = DateTimePattern::try_from_pattern_str(pattern).map_err(icu_error)?;
        let prefs = locale.locale.clone().into();
        let names = if locale.japanese {
            let mut names = FixedCalendarDateTimeNames::try_new(prefs).map_err(icu_error)?;
            names.include_for_pattern(&pattern).map_err(icu_error)?;
            Names::Japanese(names)
        } else {
            let mut names = FixedCalendarDateTimeNames::try_new(prefs).map_err(icu_error)?;
            names.include_for_pattern(&pattern).map_err(icu_error)?;
            Names::Gregorian(names)
        };
        Ok(Self { pattern, names })
    }

    pub fn format(&self, dt: NaiveDateTime, zone: &str, offset: i32) -> Result<String> {
        let iso =
            Date::try_new_iso(dt.year(), dt.month() as u8, dt.day() as u8).map_err(icu_error)?;
        let time = Time::try_new(
            dt.hour() as u8,
            dt.minute() as u8,
            dt.second() as u8,
            dt.nanosecond(),
        )
        .map_err(icu_error)?;
        let zone = TimeZone::from_iana_id(zone)
            .with_offset(Some(
                UtcOffset::try_from_seconds(offset).map_err(icu_error)?,
            ))
            .at_date_time(DateTime { date: iso, time });
        let text = match &self.names {
            Names::Gregorian(names) => names
                .with_pattern_unchecked(&self.pattern)
                .format(&ZonedDateTime {
                    date: iso.to_calendar(Gregorian),
                    time,
                    zone,
                })
                .try_write_to_string()
                .map_err(|(error, _)| icu_error(error))?
                .into_owned(),
            Names::Japanese(names) => {
                let date = iso.to_calendar(Japanese::new());
                if matches!(date.era_year().era.as_str(), "ce" | "bce") {
                    return Err(Error::sql("Japanese calendar date precedes the Meiji era"));
                }
                names
                    .with_pattern_unchecked(&self.pattern)
                    .format(&ZonedDateTime { date, time, zone })
                    .try_write_to_string()
                    .map_err(|(error, _)| icu_error(error))?
                    .into_owned()
            }
        };
        Ok(text)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Era {
    code: String,
    names: [String; 3],
    aliases: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct Symbols {
    pub japanese: bool,
    pub periods: [String; 2],
    eras: Vec<Era>,
}

impl Symbols {
    pub fn new(locale: &CalendarLocale) -> Result<Self> {
        let period = Formatter::new("a", locale)?;
        let sample = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let periods =
            [1, 13].map(|hour| period.format(sample.and_hms_opt(hour, 0, 0).unwrap(), "UTC", 0));
        let [am, pm] = periods;
        let mut symbols = Self {
            japanese: locale.japanese,
            periods: [am?, pm?],
            eras: Vec::new(),
        };
        let formatters = ["G", "GGGG", "GGGGG"].map(|p| Formatter::new(p, locale));
        let [abbr, wide, narrow] = formatters;
        let formatters = [abbr?, wide?, narrow?];
        let codes: &[(&str, &str)] = if locale.japanese {
            &[
                ("meiji", "M"),
                ("taisho", "T"),
                ("showa", "S"),
                ("heisei", "H"),
                ("reiwa", "R"),
            ]
        } else {
            &[("ce", "AD"), ("bce", "BC")]
        };
        for &(code, alias) in codes {
            // A date safely inside the era; the start and end dates come entirely from ICU.
            let year = YearInput::EraYear(code, 10);
            let iso = if locale.japanese {
                Date::try_new(year, 7.into(), 1, Japanese::new())
                    .map_err(icu_error)?
                    .to_calendar(Iso)
            } else {
                Date::try_new(year, 7.into(), 1, Gregorian)
                    .map_err(icu_error)?
                    .to_calendar(Iso)
            };
            let dt = NaiveDate::from_ymd_opt(iso.year().extended_year(), 7, 1)
                .ok_or_else(|| Error::sql("ICU era date out of range"))?
                .and_hms_opt(0, 0, 0)
                .unwrap();
            let names = [
                formatters[0].format(dt, "UTC", 0)?,
                formatters[1].format(dt, "UTC", 0)?,
                formatters[2].format(dt, "UTC", 0)?,
            ];
            let mut aliases = names.to_vec();
            aliases.push(alias.into());
            if locale.japanese && locale.locale.id.language.as_str() == "ja" {
                aliases.push(names[1].chars().take(1).collect());
            }
            symbols.eras.push(Era {
                code: code.into(),
                names,
                aliases,
            });
        }
        Ok(symbols)
    }

    pub fn era_regex(&self) -> String {
        let mut aliases: Vec<_> = self.eras.iter().flat_map(|e| &e.aliases).collect();
        aliases.sort_by_key(|s| std::cmp::Reverse(s.len()));
        aliases.dedup();
        aliases
            .into_iter()
            .map(|s| regex::escape(s))
            .collect::<Vec<_>>()
            .join("|")
    }

    pub fn resolve_date(
        &self,
        year: i32,
        month: u32,
        day: u32,
        era: Option<&str>,
        extended: bool,
    ) -> Result<NaiveDate> {
        let bad = || Error::input("invalid calendar date or era boundary");
        let era = era
            .map(|name| {
                self.eras
                    .iter()
                    .find(|e| e.aliases.iter().any(|a| a == name))
                    .ok_or_else(bad)
            })
            .transpose()?;
        let year_input = if extended {
            YearInput::Extended(year)
        } else if let Some(era) = era {
            YearInput::EraYear(&era.code, year)
        } else {
            YearInput::Extended(year)
        };
        let month = u8::try_from(month).map_err(|_| bad())?;
        let day = u8::try_from(day).map_err(|_| bad())?;
        let (iso, actual) = if self.japanese {
            let date =
                Date::try_new(year_input, month.into(), day, Japanese::new()).map_err(|_| bad())?;
            (date.to_calendar(Iso), date.era_year())
        } else {
            let date =
                Date::try_new(year_input, month.into(), day, Gregorian).map_err(|_| bad())?;
            (date.to_calendar(Iso), date.era_year())
        };
        if let Some(era) = era
            && (actual.era.as_str() != era.code || (!extended && actual.year != year))
        {
            return Err(bad());
        }
        if self.japanese && matches!(actual.era.as_str(), "ce" | "bce") {
            return Err(bad());
        }
        NaiveDate::from_ymd_opt(
            iso.year().extended_year(),
            u32::from(iso.month().ordinal),
            u32::from(iso.day_of_month().0),
        )
        .ok_or_else(bad)
    }

    pub fn era_year(&self, date: NaiveDate) -> Result<(i32, &Era)> {
        let iso = Date::try_new_iso(date.year(), date.month() as u8, date.day() as u8)
            .map_err(icu_error)?;
        let year = if self.japanese {
            iso.to_calendar(Japanese::new()).era_year()
        } else {
            iso.to_calendar(Gregorian).era_year()
        };
        let era = self
            .eras
            .iter()
            .find(|e| e.code == year.era.as_str())
            .ok_or_else(|| Error::output("calendar era out of range"))?;
        Ok((year.year, era))
    }
}

impl Era {
    pub fn name(&self, width: usize) -> &str {
        &self.names[if width <= 3 {
            0
        } else if width == 4 {
            1
        } else {
            2
        }]
    }
}
