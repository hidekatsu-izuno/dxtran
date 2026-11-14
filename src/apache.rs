//! Compile Apache LogFormat directives into bounded, anchored record matching.
use crate::{
    config::Apache,
    error::{Error, Result},
};
use regex::{CaptureLocations, Regex};

pub(crate) enum LogFormat {
    Common,
    Combined,
    Custom(CustomFormat),
}

impl LogFormat {
    pub(crate) fn columns(&self) -> usize {
        match self {
            Self::Common => 7,
            Self::Combined => 9,
            Self::Custom(format) => format.fields.len(),
        }
    }
}

impl Apache {
    pub(crate) fn compile(&self) -> Result<LogFormat> {
        if self.format != "custom" && self.log_format.is_some() {
            return Err(Error::config(
                "apache.log_format requires apache.format: custom",
            ));
        }
        match self.format.as_str() {
            "common" => Ok(LogFormat::Common),
            "combined" => Ok(LogFormat::Combined),
            "custom" => {
                let text = self
                    .log_format
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        Error::config("apache.log_format is required for apache.format: custom")
                    })?;
                Ok(LogFormat::Custom(CustomFormat::compile(text)?))
            }
            _ => Err(Error::config(
                "apache.format must be common, combined or custom",
            )),
        }
    }
}

struct Directive {
    code: String,
    argument: Option<String>,
    conditional: bool,
}

impl Directive {
    fn escaped(&self) -> bool {
        matches!(
            self.code.as_str(),
            "C" | "e"
                | "f"
                | "h"
                | "H"
                | "i"
                | "l"
                | "m"
                | "n"
                | "o"
                | "q"
                | "r"
                | "R"
                | "u"
                | "U"
                | "v"
                | "V"
                | "^ti"
                | "^to"
        )
    }
    fn default_time(&self) -> bool {
        self.code == "t" && self.argument.is_none()
    }
}

enum Token {
    Literal(String),
    Field(Directive),
}

pub(crate) struct CustomFormat {
    regex: Regex,
    locations: CaptureLocations,
    fields: Vec<Directive>,
}

impl CustomFormat {
    fn compile(text: &str) -> Result<Self> {
        let tokens = tokenize(text)?;
        let mut pattern = String::from(r"\A");
        for (index, token) in tokens.iter().enumerate() {
            match token {
                Token::Literal(text) => literal_pattern(text, &mut pattern),
                Token::Field(field) => {
                    if let Some(Token::Field(next)) = tokens.get(index + 1)
                        && !(field.code == "U" && next.code == "q")
                    {
                        return Err(Error::config(
                            "adjacent Apache directives require a literal separator (except %U%q)",
                        ));
                    }
                    let quoted = index > 0
                        && matches!(&tokens[index - 1], Token::Literal(s) if s.ends_with('"'))
                        && matches!(tokens.get(index + 1), Some(Token::Literal(s)) if s.starts_with('"'));
                    field_pattern(field, quoted, &mut pattern);
                }
            }
        }
        pattern.push_str(r"\z");
        let fields: Vec<_> = tokens
            .into_iter()
            .filter_map(|token| match token {
                Token::Field(field) => Some(field),
                Token::Literal(_) => None,
            })
            .collect();
        if fields.is_empty() {
            return Err(Error::config(
                "apache.log_format must contain at least one value directive",
            ));
        }
        let regex = Regex::new(&pattern)
            .map_err(|e| Error::config(format!("invalid apache.log_format: {e}")))?;
        let locations = regex.capture_locations();
        Ok(Self {
            regex,
            locations,
            fields,
        })
    }

    pub(crate) fn parse(
        &mut self,
        line: &str,
        mut emit: impl FnMut(&str, bool, bool) -> Result<()>,
    ) -> Result<()> {
        if self
            .regex
            .captures_read(&mut self.locations, line)
            .is_none()
        {
            return Err(Error::input(
                "Apache log record does not match apache.log_format",
            ));
        }
        for (index, field) in self.fields.iter().enumerate() {
            let (start, end) = self
                .locations
                .get(index + 1)
                .expect("one capture per directive");
            let mut value = &line[start..end];
            if field.default_time() && value.starts_with('[') {
                value = &value[1..value.len() - 1];
            }
            // %X uses '-' as a connection status. Conditions still use it
            // as the missing-value marker, just as for other directives.
            let present = value != "-" || field.code == "X" && !field.conditional;
            emit(value, field.escaped(), present)?;
        }
        Ok(())
    }
}

fn literal_pattern(text: &str, pattern: &mut String) {
    let mut rest = text;
    while let Some(index) = rest.find([' ', '\t']) {
        pattern.push_str(&regex::escape(&rest[..index]));
        pattern.push_str(r"[ \t]+");
        rest = rest[index..].trim_start_matches([' ', '\t']);
    }
    pattern.push_str(&regex::escape(rest));
}

fn field_pattern(field: &Directive, quoted: bool, pattern: &mut String) {
    pattern.push('(');
    if field.default_time() {
        pattern.push_str(r"\[[^\]\\]*\]|-");
    } else if matches!(
        field.code.as_str(),
        "b" | "B" | "D" | "k" | "p" | "s" | "T" | "I" | "O" | "S"
    ) || field.code == "P" && field.argument.as_deref() != Some("hextid")
    {
        pattern.push_str(r"[0-9]+|-");
    } else if field.code == "P" {
        pattern.push_str(r"[0-9a-fA-F]+|-");
    } else if field.code == "X" {
        pattern.push_str(r"[-+X]");
    } else if field.escaped() {
        // An escaped delimiter belongs to the field, never the surrounding
        // format. Rust's regex engine matches without recursive backtracking.
        if field.code == "q" {
            pattern.push_str(r"(?:\?");
        }
        pattern.push_str(r#"(?:\\(?:x[0-9a-fA-F]{2}|["\\abfnrtv])|[^\\"#);
        if quoted {
            pattern.push('"');
        }
        if field.code == "U" {
            pattern.push('?');
        }
        pattern.push_str(r"])*?");
        if field.code == "q" {
            pattern.push_str(")?");
            if field.conditional {
                pattern.push_str("|-");
            }
        }
    } else {
        pattern.push_str(r"[^\r\n]*?");
    }
    pattern.push(')');
}

fn tokenize(text: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut literal = String::new();
    let mut rest = text;
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix("%%") {
            literal.push('%');
            rest = after;
        } else if let Some(after) = rest.strip_prefix('%') {
            if !literal.is_empty() {
                tokens.push(Token::Literal(std::mem::take(&mut literal)));
            }
            let (field, after) = directive(after)?;
            tokens.push(Token::Field(field));
            rest = after;
        } else if let Some(after) = rest.strip_prefix('\\') {
            let c = after
                .chars()
                .next()
                .ok_or_else(|| Error::config("truncated escape in apache.log_format"))?;
            literal.push(match c {
                't' => '\t',
                '"' => '"',
                '\\' => '\\',
                _ => return Err(Error::config(r#"apache.log_format supports only \t, \" and \\ escapes and one physical line"#)),
            });
            rest = &after[c.len_utf8()..];
        } else {
            let c = rest.chars().next().unwrap();
            if c.is_control() && c != '\t' {
                return Err(Error::config(
                    "apache.log_format must describe one physical line",
                ));
            }
            literal.push(c);
            rest = &rest[c.len_utf8()..];
        }
    }
    if !literal.is_empty() {
        tokens.push(Token::Literal(literal));
    }
    Ok(tokens)
}

fn directive(mut rest: &str) -> Result<(Directive, &str)> {
    let bad = || Error::config("invalid or unsupported directive in apache.log_format");
    let mut conditional = false;
    let mut direction = false;
    loop {
        if rest.starts_with(['<', '>']) {
            if direction {
                return Err(bad());
            }
            direction = true;
            rest = &rest[1..];
        } else if rest.starts_with('!') || rest.as_bytes().first().is_some_and(u8::is_ascii_digit) {
            if conditional {
                return Err(bad());
            }
            conditional = true;
            rest = rest.strip_prefix('!').unwrap_or(rest);
            loop {
                let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
                if digits != 3 || !("100"..="599").contains(&&rest[..digits]) {
                    return Err(bad());
                }
                rest = &rest[digits..];
                if let Some(after) = rest.strip_prefix(',') {
                    rest = after;
                } else {
                    break;
                }
            }
        } else {
            break;
        }
    }
    let argument = if let Some(after) = rest.strip_prefix('{') {
        let end = after.find('}').ok_or_else(bad)?;
        let argument = &after[..end];
        if argument.is_empty() || argument.contains('{') || argument.chars().any(char::is_control) {
            return Err(bad());
        }
        rest = &after[end + 1..];
        Some(argument.to_owned())
    } else {
        None
    };
    let length = if rest.starts_with("^ti") || rest.starts_with("^to") {
        3
    } else {
        1
    };
    let code = rest.get(..length).ok_or_else(bad)?;
    let valid = matches!(
        (code, argument.as_deref()),
        ("C" | "e" | "i" | "n" | "o" | "^ti" | "^to" | "t", Some(_))
            | ("a" | "h", Some("c"))
            | ("p", Some("canonical" | "local" | "remote"))
            | ("P", Some("pid" | "tid" | "hextid"))
            | ("T", Some("s" | "ms" | "us"))
            | (
                "a" | "A"
                    | "B"
                    | "b"
                    | "D"
                    | "f"
                    | "h"
                    | "H"
                    | "k"
                    | "l"
                    | "L"
                    | "m"
                    | "p"
                    | "P"
                    | "q"
                    | "r"
                    | "R"
                    | "s"
                    | "t"
                    | "T"
                    | "u"
                    | "U"
                    | "v"
                    | "V"
                    | "X"
                    | "I"
                    | "O"
                    | "S",
                None,
            )
    );
    if !valid {
        return Err(bad());
    }
    Ok((
        Directive {
            code: code.into(),
            argument,
            conditional,
        },
        &rest[length..],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(format: &str, line: &str) -> Vec<String> {
        let mut format = CustomFormat::compile(format).unwrap();
        let mut values = Vec::new();
        format
            .parse(line, |value, _, _| {
                values.push(value.to_owned());
                Ok(())
            })
            .unwrap();
        values
    }

    #[test]
    fn matches_literals_default_and_formatted_times_and_repeated_directives() {
        assert_eq!(
            values(
                r#"[%v:%{local}p] %h %t "%r" %>s %b %D "%{Referer}i" %%"#,
                r#"[example.com:443] 127.0.0.1 [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.1" 200 12 500 "https://example.com/" %"#,
            ),
            [
                "example.com",
                "443",
                "127.0.0.1",
                "10/Oct/2000:13:55:36 -0700",
                "GET / HTTP/1.1",
                "200",
                "12",
                "500",
                "https://example.com/"
            ]
        );
        assert_eq!(
            values(
                r#"[%{%Y-%m-%d %H:%M:%S}t.%{msec_frac}t] %h %h"#,
                "[2026-11-14 20:00:00.123] a b",
            ),
            ["2026-11-14 20:00:00", "123", "a", "b"]
        );
        assert_eq!(
            values(
                r#"log.+(%h)\t\"%{User-Agent}i\""#,
                "log.+(127.0.0.1)\t\"Browser\\\"test\""
            ),
            ["127.0.0.1", "Browser\\\"test"]
        );
    }

    #[test]
    fn adjacent_path_query_fields_have_a_deterministic_boundary() {
        for (request, query) in [
            ("GET /index?id=1 HTTP/2", "?id=1"),
            ("GET /index HTTP/2", ""),
        ] {
            assert_eq!(
                values("%m %U%q %H", request),
                ["GET", "/index", query, "HTTP/2"]
            );
        }
    }

    #[test]
    fn conditions_modifiers_trailers_and_connection_status_keep_missing_metadata() {
        let mut format = CustomFormat::compile(
            "%X %200X %!200,304{Referer}i %<400s %{Trace}^ti %{hextid}P %!200t",
        )
        .unwrap();
        let mut cells = Vec::new();
        format
            .parse("- - - 400 trace abf -", |value, escaped, present| {
                cells.push((value.to_owned(), escaped, present));
                Ok(())
            })
            .unwrap();
        assert_eq!(cells[0], ("-".into(), false, true));
        assert_eq!(cells[1], ("-".into(), false, false));
        assert_eq!(cells[2], ("-".into(), true, false));
        assert_eq!(cells[4], ("trace".into(), true, true));
        assert_eq!(cells[6], ("-".into(), false, false));
    }

    #[test]
    fn malformed_and_ambiguous_formats_are_rejected() {
        for format in [
            "",
            "literal %%",
            "%",
            "%!s",
            "%20s",
            "%600s",
            "%200,s",
            "%200,099s",
            "%>>s",
            "%{unterminated",
            "%{}i",
            "%{header}z",
            "%i",
            "%^ti",
            "%{bad}p",
            "%{bad}P",
            "%{bad}T",
            "%{bad}a",
            "%h%l",
            "%r%r",
            "%h\n%s",
            r"%h\n%s",
            r"%h\q",
            "%h\\",
            "%{\n}t",
            "%{{x}}t",
        ] {
            assert!(CustomFormat::compile(format).is_err(), "{format}");
        }
    }

    #[test]
    fn records_must_match_all_literals_quotes_and_numeric_directives() {
        for line in [
            r#"host GET / HTTP/1.1 200"#,
            r#"host "GET / HTTP/1.1" bad"#,
            r#"host "GET / HTTP/1.1" 200 extra"#,
            r#"host "GET "bad" / HTTP/1.1" 200"#,
            r#"host "bad\q" 200"#,
        ] {
            let mut format = CustomFormat::compile(r#"%h "%r" %>s"#).unwrap();
            assert!(format.parse(line, |_, _, _| Ok(())).is_err(), "{line}");
        }
    }
}
