use crate::error::{Error, Result};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};
use yaml_rust2::{
    Yaml,
    parser::{Event, Parser},
    scanner::{Marker, TScalarStyle},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Origin {
    pub file: PathBuf,
    pub line: usize,
    pub column: usize,
}
impl std::fmt::Display for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.file.display(), self.line, self.column)
    }
}
#[derive(Clone, Debug)]
enum Kind {
    Scalar(Value),
    Sequence(Vec<Node>),
    Mapping(BTreeMap<String, Node>),
}
#[derive(Clone, Debug)]
struct Node {
    kind: Kind,
    origin: Origin,
}
impl Node {
    fn value(&self) -> Value {
        match &self.kind {
            Kind::Scalar(v) => v.clone(),
            Kind::Sequence(v) => Value::Array(v.iter().map(Node::value).collect()),
            Kind::Mapping(v) => {
                Value::Object(v.iter().map(|(k, v)| (k.clone(), v.value())).collect())
            }
        }
    }
    fn get(&self, key: &str) -> Option<&Node> {
        if let Kind::Mapping(m) = &self.kind {
            m.get(key)
        } else {
            None
        }
    }
    fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.value())
            .map_err(|e| Error::config(e.to_string()).context(&self.origin))
    }
}

fn parse_node(events: &[(Event, Marker)], index: &mut usize, file: &Path) -> Result<Node> {
    let (event, mark) = &events[*index];
    let origin = Origin {
        file: file.into(),
        line: mark.line(),
        column: mark.col() + 1,
    };
    *index += 1;
    let forbidden =
        || Error::config("YAML tags, anchors and aliases are forbidden").context(&origin);
    let kind = match event {
        Event::Scalar(s, style, anchor, tag) => {
            if *anchor != 0 || tag.is_some() {
                return Err(forbidden());
            }
            let value = if *style != TScalarStyle::Plain {
                Value::String(s.clone())
            } else {
                match Yaml::from_str(s) {
                    Yaml::Null => Value::Null,
                    Yaml::Boolean(v) => Value::Bool(v),
                    Yaml::Integer(v) => Value::from(v),
                    Yaml::Real(v) => {
                        let n = v
                            .parse::<f64>()
                            .ok()
                            .and_then(serde_json::Number::from_f64)
                            .ok_or_else(|| {
                                Error::config("non-finite YAML number").context(&origin)
                            })?;
                        Value::Number(n)
                    }
                    _ => Value::String(s.clone()),
                }
            };
            Kind::Scalar(value)
        }
        Event::SequenceStart(anchor, tag) => {
            if *anchor != 0 || tag.is_some() {
                return Err(forbidden());
            }
            let mut nodes = vec![];
            while !matches!(events[*index].0, Event::SequenceEnd) {
                nodes.push(parse_node(events, index, file)?);
            }
            *index += 1;
            Kind::Sequence(nodes)
        }
        Event::MappingStart(anchor, tag) => {
            if *anchor != 0 || tag.is_some() {
                return Err(forbidden());
            }
            let mut nodes = BTreeMap::new();
            while !matches!(events[*index].0, Event::MappingEnd) {
                let key = parse_node(events, index, file)?;
                let Value::String(k) = key.value() else {
                    return Err(Error::config("mapping key must be a string").context(&key.origin));
                };
                let value = parse_node(events, index, file)?;
                if nodes.insert(k.clone(), value).is_some() {
                    return Err(
                        Error::config(format!("duplicate YAML key '{k}'")).context(&key.origin)
                    );
                }
            }
            *index += 1;
            Kind::Mapping(nodes)
        }
        _ => return Err(forbidden()),
    };
    Ok(Node { kind, origin })
}

fn parse(text: &str, file: &Path) -> Result<Node> {
    let mut parser = Parser::new_from_str(text);
    let mut events = vec![];
    loop {
        let e = parser
            .next_token()
            .map_err(|e| Error::config(e.to_string()).context(file.display()))?;
        let end = e.0 == Event::StreamEnd;
        events.push(e);
        if end {
            break;
        }
    }
    if events
        .iter()
        .filter(|(e, _)| *e == Event::DocumentStart)
        .count()
        != 1
    {
        return Err(Error::config("exactly one YAML document is required").context(file.display()));
    }
    let mut index = 2;
    parse_node(&events, &mut index, file)
}
fn merge(base: &mut Node, local: Node) {
    if let (Kind::Mapping(a), Kind::Mapping(b)) = (&mut base.kind, &local.kind) {
        for (k, v) in b {
            if let Some(old) = a.get_mut(k) {
                merge(old, v.clone());
            } else {
                a.insert(k.clone(), v.clone());
            }
        }
    } else {
        *base = local;
    }
}

#[derive(Default)]
struct Loader {
    cache: BTreeMap<PathBuf, Node>,
    hashes: BTreeMap<PathBuf, String>,
    stack: Vec<PathBuf>,
}
impl Loader {
    fn file(&mut self, path: &Path) -> Result<Node> {
        let path = fs::canonicalize(path).map_err(|e| {
            Error::config(format!(
                "{}: {e}; import chain: {:?}",
                path.display(),
                self.stack
            ))
        })?;
        if self.stack.contains(&path) || self.stack.len() >= 64 {
            return Err(Error::config(format!(
                "cyclic import or depth exceeds 64: {:?} -> {}",
                self.stack,
                path.display()
            )));
        }
        let node = if let Some(n) = self.cache.get(&path) {
            n.clone()
        } else {
            let bytes = fs::read(&path)
                .map_err(|e| Error::config(e.to_string()).context(path.display()))?;
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| Error::config("YAML must be UTF-8").context(path.display()))?;
            let n = parse(text, &path)?;
            self.hashes
                .insert(path.clone(), hex::encode(Sha256::digest(&bytes)));
            self.cache.insert(path.clone(), n.clone());
            n
        };
        self.stack.push(path);
        let result = self.expand(node);
        self.stack.pop();
        result
    }
    fn expand(&mut self, mut node: Node) -> Result<Node> {
        match &mut node.kind {
            Kind::Mapping(map) => {
                let imported = map.remove("import");
                for child in map.values_mut() {
                    *child = self.expand(child.clone())?;
                }
                if let Some(import) = imported {
                    let Value::String(path) = import.value() else {
                        return Err(
                            Error::config("import must be a file path").context(import.origin)
                        );
                    };
                    if path.contains("://") {
                        return Err(
                            Error::config("remote imports are forbidden").context(import.origin)
                        );
                    }
                    let path = import.origin.file.parent().unwrap().join(path);
                    let mut base = self
                        .file(&path)
                        .map_err(|e| e.context(format!("{} import", import.origin)))?;
                    if map.is_empty() {
                        return Ok(base);
                    }
                    if !matches!(base.kind, Kind::Mapping(_)) {
                        return Err(Error::config("cannot merge keys into a non-mapping import")
                            .context(import.origin));
                    }
                    merge(&mut base, node);
                    return Ok(base);
                }
            }
            Kind::Sequence(nodes) => {
                for n in nodes {
                    *n = self.expand(n.clone())?;
                }
            }
            _ => (),
        }
        Ok(node)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Csv,
    Fixed,
    Rdw,
    Parquet,
}
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    Gzip,
    Zstd,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Parquet {
    #[serde(default = "parquet_compression")]
    pub compression: String,
    #[serde(default = "parquet_row_group_size")]
    pub row_group_size: usize,
}
fn parquet_compression() -> String {
    "snappy".into()
}
fn parquet_row_group_size() -> usize {
    8192
}
impl Default for Parquet {
    fn default() -> Self {
        Self {
            compression: parquet_compression(),
            row_group_size: parquet_row_group_size(),
        }
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Csv {
    #[serde(default = "yes")]
    pub header: bool,
    #[serde(default = "comma")]
    pub delimiter: String,
    #[serde(default = "quote")]
    pub quote: String,
    #[serde(default)]
    pub line_ending: Option<String>,
    #[serde(default)]
    pub quote_style: Option<String>,
}
fn yes() -> bool {
    true
}
fn comma() -> String {
    ",".into()
}
fn quote() -> String {
    "\"".into()
}
impl Default for Csv {
    fn default() -> Self {
        Self {
            header: true,
            delimiter: comma(),
            quote: quote(),
            line_ending: None,
            quote_style: None,
        }
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixed {
    pub record_separator: String,
    pub record_length: Option<usize>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rdw {
    #[serde(default = "rdw_max")]
    pub max_record_length: usize,
}
fn rdw_max() -> usize {
    32760
}
impl Default for Rdw {
    fn default() -> Self {
        Self {
            max_record_length: rdw_max(),
        }
    }
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Length {
    Fixed(usize),
    Rest(String),
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Bit {
    pub name: String,
    pub bit: usize,
}
#[derive(Clone, Debug, Deserialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub name: Option<String>,
    pub offset: Option<usize>,
    pub length: Option<Length>,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub precision: Option<u8>,
    pub scale: Option<u8>,
    pub zone: Option<String>,
    pub nullable: Option<bool>,
    pub trim: Option<String>,
    pub pad_char: Option<String>,
    pub null_if: Option<Vec<String>>,
    pub null_if_pattern: Option<String>,
    pub null_output: Option<String>,
    pub null_bytes: Option<String>,
    pub max_length: Option<usize>,
    pub fill_byte: Option<u8>,
    pub true_values: Option<Vec<String>>,
    pub false_values: Option<Vec<String>>,
    pub true_output: Option<String>,
    pub false_output: Option<String>,
    pub datetime_pattern: Option<String>,
    pub two_digit_year_start: Option<i32>,
    pub unit: Option<String>,
    pub timezone: Option<String>,
    pub binary_encoding: Option<String>,
    pub quote_style: Option<String>,
    pub bits: Option<Vec<Bit>>,
    pub bit_order: Option<String>,
    #[serde(skip)]
    pub timezone_set: bool,
    #[serde(skip)]
    pub origin: Option<Origin>,
    #[serde(skip)]
    pub origins: BTreeMap<String, Origin>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    pub id: String,
    pub format: Format,
    pub path: PathBuf,
    pub compression: Option<Compression>,
    #[serde(default)]
    pub encoding: String,
    #[serde(default)]
    pub fields: Vec<Field>,
    pub csv: Option<Csv>,
    pub fixed: Option<Fixed>,
    pub rdw: Option<Rdw>,
    pub parquet: Option<Parquet>,
    pub sql: Option<String>,
    pub mode: Option<String>,
    #[serde(skip)]
    pub origin: Option<Origin>,
    #[serde(skip)]
    pub sql_origin: Option<Origin>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
    pub id: String,
    pub sql: String,
    #[serde(skip)]
    pub origin: Option<Origin>,
}
#[derive(Clone, Debug)]
pub struct Job {
    pub input: Vec<Dataset>,
    pub process: Vec<Process>,
    pub output: Vec<Dataset>,
    pub hashes: BTreeMap<PathBuf, String>,
    pub expanded_hash: String,
}
#[derive(Default, Debug)]
pub struct PathOverrides {
    pub input: BTreeMap<String, PathBuf>,
    pub output: BTreeMap<String, PathBuf>,
}
impl PathOverrides {
    fn apply(&self, root: &mut Node) -> Result<()> {
        for (section, paths) in [("input", &self.input), ("output", &self.output)] {
            if paths.is_empty() {
                continue;
            }
            let Kind::Mapping(map) = &mut root.kind else {
                unreachable!("job root was validated");
            };
            let node = map.get_mut(section).unwrap();
            let Kind::Sequence(datasets) = &mut node.kind else {
                return Err(Error::config("expected array").context(&node.origin));
            };
            for (id, path) in paths {
                let option = format!("--{section}.{id}.path");
                if path.as_os_str().is_empty() {
                    return Err(Error::config(format!("{option}: path must not be empty")));
                }
                let dataset = datasets
                    .iter_mut()
                    .find(|n| n.get("id").is_some_and(|n| n.value().as_str() == Some(id)))
                    .ok_or_else(|| {
                        Error::config(format!("{option}: unknown {section} id '{id}'"))
                    })?;
                let path = if path.is_absolute() {
                    path.clone()
                } else {
                    std::env::current_dir()
                        .map_err(|e| Error::config(e.to_string()).context(&option))?
                        .join(path)
                };
                let value = serde_json::to_value(path)
                    .map_err(|e| Error::config(e.to_string()).context(&option))?;
                let Kind::Mapping(map) = &mut dataset.kind else {
                    unreachable!("dataset has an id");
                };
                map.insert(
                    "path".into(),
                    Node {
                        kind: Kind::Scalar(value),
                        origin: Origin {
                            file: "<command line>".into(),
                            line: 1,
                            column: 1,
                        },
                    },
                );
            }
        }
        Ok(())
    }
}
pub fn identifier(name: &str) -> bool {
    let mut chars = name.bytes();
    matches!(chars.next(), Some(b'a'..=b'z'))
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
}
fn datasets(node: &Node, output: bool) -> Result<Vec<Dataset>> {
    let Kind::Sequence(seq) = &node.kind else {
        return Err(Error::config("expected array").context(&node.origin));
    };
    seq.iter()
        .map(|n| {
            let path = n.get("path").ok_or_else(|| {
                let section = if output { "output" } else { "input" };
                let id = n.get("id").map(Node::value).unwrap_or(Value::Null);
                let id = id.as_str().unwrap_or("<id>");
                Error::config(format!(
                    "{section}.{id}.path is required in YAML or via --{section}.{id}.path"
                ))
                .context(&n.origin)
            })?;
            let mut d: Dataset = n.decode()?;
            d.origin = Some(n.origin.clone());
            d.sql_origin = n.get("sql").map(|n| n.origin.clone());
            if d.path.as_os_str().is_empty() {
                return Err(Error::config("path must not be empty").context(&path.origin));
            }
            d.path = path.origin.file.parent().unwrap().join(&d.path);
            if let Some(Node {
                kind: Kind::Sequence(fields),
                ..
            }) = n.get("fields")
            {
                for (field, raw) in d.fields.iter_mut().zip(fields) {
                    field.origin = Some(raw.origin.clone());
                    if let Kind::Mapping(m) = &raw.kind {
                        field.origins = m
                            .iter()
                            .map(|(k, v)| (k.clone(), v.origin.clone()))
                            .collect();
                    }
                    field.timezone_set = raw.get("timezone").is_some();
                }
            }
            if !identifier(&d.id) {
                return Err(Error::config("invalid dataset id").context(&n.origin));
            }
            if output && d.sql.is_none() {
                return Err(Error::config("output.sql is required").context(&n.origin));
            }
            if !output && (d.sql.is_some() || d.mode.is_some()) {
                return Err(Error::config("sql/mode are output-only").context(&n.origin));
            }
            if d.format == Format::Parquet {
                for key in ["encoding", "fields"] {
                    if let Some(node) = n.get(key) {
                        return Err(Error::config(format!(
                            "parquet {key} is read from the file or SQL schema; do not specify it"
                        ))
                        .context(&node.origin));
                    }
                }
            } else if n.get("encoding").is_none() {
                return Err(Error::config("encoding is required").context(&n.origin));
            }
            if !output && d.format != Format::Parquet && d.fields.is_empty() {
                return Err(Error::config("input.fields must not be empty").context(&n.origin));
            }
            Ok(d)
        })
        .collect()
}
pub fn load(path: &Path) -> Result<Job> {
    load_with_paths(path, &PathOverrides::default())
}
pub fn load_with_paths(path: &Path, paths: &PathOverrides) -> Result<Job> {
    let mut loader = Loader::default();
    let mut root = loader.file(path)?;
    fn validate_nulls(node: &Node, allow: bool) -> Result<()> {
        match &node.kind {
            Kind::Scalar(Value::Null) if !allow => {
                return Err(
                    Error::config("null is only allowed for timezone").context(&node.origin)
                );
            }
            Kind::Mapping(m) => {
                for (k, v) in m {
                    validate_nulls(v, k == "timezone")?;
                }
            }
            Kind::Sequence(s) => {
                for v in s {
                    validate_nulls(v, false)?;
                }
            }
            _ => (),
        };
        Ok(())
    }
    validate_nulls(&root, false)?;
    let Kind::Mapping(m) = &root.kind else {
        return Err(Error::config("job must be a mapping").context(&root.origin));
    };
    if !m.contains_key("input")
        || !m.contains_key("output")
        || m.keys()
            .any(|k| !matches!(k.as_str(), "input" | "process" | "output"))
    {
        return Err(Error::config(
            "job requires input and output; process is optional and other keys are forbidden",
        )
        .context(&root.origin));
    }
    paths.apply(&mut root)?;
    let Kind::Mapping(m) = &root.kind else {
        unreachable!("job root was validated");
    };
    let input = datasets(&m["input"], false)?;
    let output = datasets(&m["output"], true)?;
    let mut process: Vec<Process> = m
        .get("process")
        .map(Node::decode)
        .transpose()?
        .unwrap_or_default();
    if let Some(Node {
        kind: Kind::Sequence(nodes),
        ..
    }) = m.get("process")
    {
        for (p, n) in process.iter_mut().zip(nodes) {
            p.origin = n.get("sql").map(|n| n.origin.clone());
        }
    }
    if input.is_empty() || output.is_empty() {
        return Err(Error::config("input and output must not be empty"));
    }
    let mut ids = BTreeSet::new();
    for id in input
        .iter()
        .map(|d| &d.id)
        .chain(process.iter().map(|d| &d.id))
    {
        if !identifier(id) || !ids.insert(id) {
            return Err(Error::config(format!(
                "invalid or duplicate table id '{id}'"
            )));
        }
    }
    ids.clear();
    for d in &output {
        if !ids.insert(&d.id) {
            return Err(Error::config(format!("duplicate output id '{}'", d.id)));
        }
    }
    Ok(Job {
        input,
        process,
        output,
        hashes: loader.hashes,
        expanded_hash: hex::encode(Sha256::digest(serde_json::to_vec(&root.value()).unwrap())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_job() -> Value {
        serde_json::json!({
            "input": [{
                "id": "data", "format": "csv", "path": "in.csv",
                "encoding": "utf-8", "fields": [{"name": "value"}]
            }],
            "output": [{
                "id": "result", "format": "csv", "path": "out.csv",
                "encoding": "utf-8", "sql": "SELECT * FROM data"
            }]
        })
    }

    #[test]
    fn process_is_optional_in_direct_and_imported_jobs() {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().join("base.yml");
        let imported = directory.path().join("job.yml");
        fs::write(&imported, "import: base.yml\n").unwrap();
        for process in [None, Some(serde_json::json!([]))] {
            let mut value = minimal_job();
            if let Some(process) = process {
                value["process"] = process;
            }
            fs::write(&base, serde_json::to_vec(&value).unwrap()).unwrap();
            for path in [&base, &imported] {
                let job = load(path).unwrap();
                assert_eq!(job.input.len(), 1);
                assert_eq!(job.output.len(), 1);
                assert!(job.process.is_empty());
            }
        }
    }

    #[test]
    fn optional_process_keeps_root_keys_and_types_strict() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("job.yml");
        let mut invalid = Vec::new();
        for process in [
            Value::Null,
            serde_json::json!({}),
            serde_json::json!(""),
            serde_json::json!(false),
            serde_json::json!(1),
        ] {
            let mut value = minimal_job();
            value["process"] = process;
            invalid.push(value);
        }
        for required in ["input", "output"] {
            let mut value = minimal_job();
            value.as_object_mut().unwrap().remove(required);
            invalid.push(value);
            let mut value = minimal_job();
            value[required] = serde_json::json!([]);
            invalid.push(value);
        }
        let mut unknown = minimal_job();
        unknown["proces"] = serde_json::json!([]);
        invalid.push(unknown);
        for value in invalid {
            fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
            assert_eq!(load(&path).unwrap_err().code, 2, "{value}");
        }
    }

    #[test]
    fn path_overrides_are_included_in_the_expanded_hash() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("job.yml");
        fs::write(&path, serde_json::to_vec(&minimal_job()).unwrap()).unwrap();
        let original = load(&path).unwrap();
        let mut paths = PathOverrides::default();
        paths.input.insert("data".into(), "actual.csv".into());
        let overridden = load_with_paths(&path, &paths).unwrap();
        assert_eq!(original.hashes, overridden.hashes);
        assert_ne!(original.expanded_hash, overridden.expanded_hash);
        assert_eq!(
            overridden.input[0].path,
            std::env::current_dir().unwrap().join("actual.csv")
        );
        assert_eq!(overridden.output[0].path, original.output[0].path);
        assert_eq!(
            overridden.expanded_hash,
            load_with_paths(&path, &paths).unwrap().expanded_hash
        );
        paths.input.insert("data".into(), "another.csv".into());
        assert_ne!(
            overridden.expanded_hash,
            load_with_paths(&path, &paths).unwrap().expanded_hash
        );
    }

    #[test]
    fn rejects_yaml_extensions_and_duplicates() {
        for s in ["a: 1\na: 2", "a: &x 1", "a: !foo x", "a: 1\n---\na: 2"] {
            assert!(parse(s, Path::new("job.yml")).is_err(), "{s}");
        }
    }
    #[test]
    fn yaml12() {
        let n = parse("a: yes\nb: true\nc: 012", Path::new("j")).unwrap();
        assert_eq!(n.value()["a"], "yes");
        assert_eq!(n.value()["b"], true);
    }
}
