//! Nested physical records. SQL sees scalar lists and lists of structs;
//! fixed records reserve the maximum extent and RDW records use actual counts.
mod condition;
mod decode;
use super::RecordLayout;
use crate::{
    config::{Dataset, Field, Format, Length, Repeat, identifier},
    encoding::TextEncoding,
    error::{Error, Result},
    field::{CompiledField, Direction, FieldType},
    framing::MAX_RECORD,
};
use condition::Condition;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, ListArray, StructArray, new_empty_array},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field as ArrowField, Fields, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    common::ScalarValue,
};
use std::{collections::BTreeMap, sync::Arc};

type Row = BTreeMap<String, ScalarValue>;
type Symbols = BTreeMap<String, DataType>;

#[derive(Clone, Debug)]
pub(crate) struct RepeatLayout {
    root: Group,
    format: Format,
}

#[derive(Clone, Debug)]
struct Group {
    nodes: Vec<Node>,
    columns: Fields,
    width: usize,
    variable: bool,
    rest: bool,
    length: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
enum Position {
    Absolute(usize),
    After(usize),
}

#[derive(Clone, Debug)]
enum Element {
    Leaf(Box<CompiledField>),
    Group(Group),
}

#[derive(Clone, Debug)]
struct Count {
    min: usize,
    max: usize,
    source: Option<(usize, String)>,
}

#[derive(Clone, Debug)]
struct Node {
    name: Option<String>,
    element: Element,
    repeat: Option<Count>,
    columns: Fields,
    element_type: DataType,
    element_nullable: bool,
    width: usize,
    position: Position,
    max_start: usize,
    variable: bool,
    rest: bool,
    when_sql: Option<String>,
    when: Option<Condition>,
    origin: String,
}

fn width_add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .filter(|n| *n <= MAX_RECORD)
        .ok_or_else(|| Error::config("repeat layout exceeds 16 MiB record limit"))
}

fn width_mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .filter(|n| *n <= MAX_RECORD)
        .ok_or_else(|| Error::config("repeat layout exceeds 16 MiB record limit"))
}

fn counter_type(t: &DataType) -> bool {
    t.is_integer() || matches!(t, DataType::Decimal128(_, 0))
}

pub(super) fn compile(
    d: &Dataset,
    result: Option<SchemaRef>,
    encoding: TextEncoding,
    separator: Vec<u8>,
    max: usize,
) -> Result<RecordLayout> {
    let direction = if result.is_some() {
        Direction::Output
    } else {
        Direction::Input
    };
    let mut root = Group::compile(&d.fields, d.format, direction, encoding, &mut Vec::new())
        .map_err(|e| e.context(&d.id))?;
    if (root.width == 0 && d.format != Format::Rdw)
        || d.format == Format::Rdw && root.width + 4 > max
    {
        return Err(Error::config(
            "repeat record length outside supported range",
        ));
    }
    if d.format == Format::Fixed
        && d.fixed
            .as_ref()
            .and_then(|f| f.record_length)
            .is_some_and(|n| n != root.width)
    {
        return Err(Error::config(
            "fixed.record_length must equal the maximum repeat layout length",
        ));
    }
    let lines = if d.format == Format::Lines {
        let options = d.lines.clone().unwrap_or_default();
        if options.record_length.is_some_and(|n| n != root.width) {
            return Err(Error::config(
                "lines.record_length must equal the maximum repeat layout length",
            ));
        }
        let lines = crate::framing::lines::LineFraming::new(encoding, &options)?;
        root.validate_lines(&lines)?;
        Some(lines)
    } else {
        None
    };
    if let Some(result) = &result {
        root.validate_output(result.fields())?;
    }
    root.bind_conditions(result.as_ref().map(|s| s.fields()), &mut Vec::new())?;
    let schema = Arc::new(Schema::new(root.columns.clone()));
    Ok(RecordLayout {
        fields: Vec::new(),
        schema,
        length: root.width,
        rest: root.variable && d.format == Format::Rdw,
        separator,
        max_record_length: max,
        format: d.format,
        encoding,
        lines,
        conditional: None,
        repeated: Some(RepeatLayout {
            root,
            format: d.format,
        }),
        checks: Default::default(),
    })
}

impl Group {
    fn compile(
        specs: &[Field],
        format: Format,
        direction: Direction,
        encoding: TextEncoding,
        scopes: &mut Vec<Symbols>,
    ) -> Result<Self> {
        if specs.is_empty() || scopes.len() >= 32 {
            return Err(Error::config(
                "nested fields must be nonempty and at most 32 levels deep",
            ));
        }
        scopes.push(Symbols::new());
        let conditional = specs.iter().any(|s| s.when.is_some());
        let mut group = Self {
            nodes: Vec::new(),
            columns: Fields::empty(),
            width: 0,
            variable: conditional && format == Format::Rdw,
            rest: false,
            length: None,
        };
        let mut columns: Vec<Arc<ArrowField>> = Vec::new();
        for spec in specs {
            let origin = format!(
                "{} field {}",
                spec.origin
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                spec.name.as_deref().unwrap_or("FILLER")
            );
            let node = (|| -> Result<Node> {
                if spec.source_name.is_some() {
                    return Err(Error::config("source_name requires CSV input"));
                }
                let previous = group.nodes.last();
                let max_start = spec.offset.unwrap_or(previous.map_or(0, |p| p.max_start + p.width));
                if !conditional && spec.offset.is_some_and(|n| n != group.width) {
                    return Err(Error::config("repeat layout fields must cover the record without gaps or overlaps; use FILLER"));
                }
                if !conditional && format == Format::Rdw && group.variable && spec.offset.is_some() {
                    return Err(Error::config("omit offset after a variable repeat in RDW; following fields use the actual preceding length"));
                }
                let position = if spec.offset.is_none() && format == Format::Rdw
                    && previous.is_some_and(|p| p.variable || matches!(p.position, Position::After(_))) {
                    Position::After(group.nodes.len() - 1)
                } else { Position::Absolute(max_start) };
                let repeat = spec.repeat.as_ref().map(|r| Count::compile(r, scopes)).transpose()?;
                let element = if let Some(children) = &spec.fields {
                    if !spec.name.as_deref().is_some_and(identifier) {
                        return Err(Error::config("nested fields require a valid group name"));
                    }
                    let length = match spec.length {
                        None => None,
                        Some(Length::Fixed(n)) if n > 0 && n <= MAX_RECORD => Some(n),
                        _ => return Err(Error::config("group length must be a positive byte length")),
                    };
                    let mut extra = spec.clone();
                    extra.name = None;
                    extra.description = None;
                    extra.offset = None;
                    extra.repeat = None;
                    extra.fields = None;
                    extra.when = None;
                    extra.length = None;
                    if extra.nullable == Some(false) { extra.nullable = None; }
                    extra.origin = None;
                    extra.origins.clear();
                    if extra != Field::default() {
                        return Err(Error::config("groups accept only name, description, offset, length, when, repeat, fields and nullable: false"));
                    }
                    let mut child = Self::compile(children, format, direction, encoding, scopes)?;
                    if let Some(length) = length {
                        if child.width > length || (!child.rest && child.width != length) {
                            return Err(Error::config("group length must cover its fields exactly; use FILLER or length: rest"));
                        }
                        child.length = Some(length);
                        child.width = length;
                        child.variable = false;
                        child.rest = false;
                    }
                    Element::Group(child)
                } else {
                    let mut scalar = spec.clone();
                    scalar.repeat = None;
                    let field = CompiledField::compile(scalar, format, direction, 0, encoding)?;
                    if repeat.is_some() && field.kind == FieldType::Filler {
                        return Err(Error::config("repeat requires a named field or group"));
                    }
                    Element::Leaf(Box::new(field))
                };
                let rest = element.rest();
                if rest && repeat.as_ref().is_some_and(|r| r.max > 1) {
                    return Err(Error::config("repeating length: rest requires a group with an explicit length to delimit every element"));
                }
                let variable = format == Format::Rdw && (rest
                    || repeat.as_ref().is_some_and(|r| r.source.is_some())
                    || matches!(&element, Element::Group(g) if g.variable));
                let element_nullable = matches!(&element, Element::Leaf(f) if f.kind != FieldType::Bits && f.spec.nullable == Some(true));
                let element_type = element.data_type();
                let columns: Fields = if repeat.is_some() {
                    vec![ArrowField::new(spec.name.as_ref().unwrap(), DataType::List(Arc::new(ArrowField::new_list_field(element_type.clone(), element_nullable))), spec.when.is_some())].into()
                } else {
                    match &element {
                        Element::Group(g) => vec![ArrowField::new(spec.name.as_ref().unwrap(), DataType::Struct(g.columns.clone()), spec.when.is_some())].into(),
                        Element::Leaf(f) => leaf_columns(f).iter().map(|c| Arc::new(c.as_ref().clone().with_nullable(c.is_nullable() || spec.when.is_some()))).collect::<Vec<_>>().into(),
                    }
                };
                let width = width_mul(element.width(), repeat.as_ref().map_or(1, |r| r.max))?;
                Ok(Node { name: spec.name.clone(), element, repeat, columns, element_type,
                    element_nullable, width, position, max_start, variable, rest,
                    when_sql: spec.when.clone(), when: None, origin: origin.clone() })
            })().map_err(|e| e.context(&origin))?;
            // Bound offsets before any overlap arithmetic, including inactive
            // alternatives whose positions may otherwise never be read.
            group.width = group.width.max(width_add(node.max_start, node.width)?);
            for column in &node.columns {
                if let Some(i) = columns.iter().position(|c| c.name() == column.name()) {
                    if columns[i].data_type() != column.data_type() {
                        return Err(Error::config(format!("conditional field '{}' must have the same logical type in every definition", column.name())).context(&origin));
                    }
                    columns[i] = Arc::new(
                        columns[i]
                            .as_ref()
                            .clone()
                            .with_nullable(columns[i].is_nullable() || column.is_nullable()),
                    );
                } else {
                    columns.push(column.clone());
                }
                scopes
                    .last_mut()
                    .unwrap()
                    .insert(column.name().clone(), column.data_type().clone());
            }
            for prior in &group.nodes {
                if prior.when_sql != node.when_sql {
                    continue;
                }
                let duplicate = prior
                    .columns
                    .iter()
                    .any(|a| node.columns.iter().any(|b| a.name() == b.name()));
                let overlap = matches!((prior.position, node.position), (Position::Absolute(a), Position::Absolute(b))
                    if !prior.variable && !node.variable && a < b + node.width && b < a + prior.width);
                if duplicate || overlap {
                    return Err(Error::config("fields active under the same condition have overlaps or duplicate column names").context(&origin));
                }
            }
            group.variable |= node.variable;
            group.rest |= node.rest;
            group.nodes.push(node);
        }
        scopes.pop();
        group.columns = columns.into();
        Ok(group)
    }

    fn validate_lines(&self, lines: &crate::framing::lines::LineFraming) -> Result<()> {
        lines.validate_width(self.width)?;
        for node in &self.nodes {
            lines.validate_width(node.element.width())?;
            if let Element::Group(group) = &node.element {
                group.validate_lines(lines)?;
            }
        }
        Ok(())
    }

    fn validate_output(&self, actual: &Fields) -> Result<()> {
        if self.columns.len() != actual.len() {
            return Err(Error::config(
                "physical output fields must match SQL fields exactly, including nested fields",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for column in actual {
            if !identifier(column.name()) || !seen.insert(column.name()) {
                return Err(Error::config(
                    "SQL output fields must be unique public identifiers within each group",
                ));
            }
        }
        for node in &self.nodes {
            if node.repeat.is_none()
                && let Element::Leaf(f) = &node.element
            {
                validate_leaf(f, actual)?;
                continue;
            }
            let name = node.name.as_ref().unwrap();
            let column = actual
                .iter()
                .find(|f| f.name() == name)
                .ok_or_else(|| Error::config(format!("missing output field '{name}'")))?;
            let t = if node.repeat.is_some() {
                list_item(column.data_type())
                    .ok_or_else(|| {
                        Error::config(format!("repeat output '{name}' requires an array"))
                    })?
                    .data_type()
            } else {
                column.data_type()
            };
            node.element.validate_type(t).map_err(|e| e.context(name))?;
        }
        Ok(())
    }

    fn validate_ranges(
        &self,
        ranges: &mut [(usize, usize, usize, bool)],
        format: Format,
        output: bool,
    ) -> Result<(usize, bool)> {
        let bad = |message| Error::new(if output { 5 } else { 3 }, message);
        ranges.sort_by_key(|(start, _, index, _)| (*start, *index));
        if ranges.is_empty() {
            return Err(bad("no matching record condition in group"));
        }
        let mut end = 0;
        let mut rest = false;
        for &(start, stop, _, tail) in ranges.iter() {
            if rest {
                return Err(bad(
                    "length: rest must be the last active field in its group",
                ));
            }
            if start != end || stop < start {
                return Err(bad(
                    "selected fields must cover the group without overlaps or gaps; use FILLER",
                ));
            }
            end = stop;
            rest = tail;
        }
        if self
            .length
            .or((format != Format::Rdw).then_some(self.width))
            .is_some_and(|n| n != end)
        {
            return Err(bad(
                "selected fields do not cover the declared group or record length",
            ));
        }
        if end > MAX_RECORD {
            return Err(bad("repeat record exceeds 16 MiB"));
        }
        Ok((end, rest && self.length.is_none()))
    }

    fn encode(
        &self,
        row: &Row,
        body: &mut Vec<u8>,
        format: Format,
        scopes: &mut Vec<Row>,
    ) -> Result<bool> {
        scopes.push(row.clone());
        let mut ends = Vec::new();
        let mut ranges = Vec::new();
        let mut chunks = BTreeMap::new();
        let mut names = std::collections::BTreeSet::new();
        for (index, node) in self.nodes.iter().enumerate() {
            let start = match node.position {
                Position::Absolute(n) => n,
                Position::After(i) => ends[i],
            };
            let active = node
                .when
                .as_ref()
                .map(|c| c.output_matches(scopes))
                .transpose()
                .map_err(|e| e.context(&node.origin))?
                .unwrap_or(true);
            if !active {
                ends.push(if node.variable {
                    start
                } else {
                    start + node.width
                });
                continue;
            }
            for field in &node.columns {
                if !names.insert(field.name()) {
                    return Err(Error::output(format!(
                        "duplicate active field '{}'",
                        field.name()
                    )));
                }
            }
            let mut bytes = Vec::new();
            let rest = node
                .encode(row, &mut bytes, format, scopes)
                .map_err(|e| e.context(&node.origin))?;
            let end = start
                .checked_add(bytes.len())
                .filter(|end| *end <= MAX_RECORD)
                .ok_or_else(|| Error::output("repeat record exceeds 16 MiB"))?;
            ends.push(end);
            ranges.push((start, end, index, rest));
            chunks.insert(index, bytes);
        }
        let (_, rest) = self.validate_ranges(&mut ranges, format, true)?;
        for (_, _, index, _) in ranges {
            body.extend(chunks.remove(&index).unwrap());
        }
        scopes.pop();
        Ok(rest)
    }

    fn has_conditions(&self) -> bool {
        self.nodes.iter().any(|n| {
            n.when_sql.is_some() || matches!(&n.element, Element::Group(g) if g.has_conditions())
        })
    }

    fn defaults(&self, body: &mut Vec<u8>, format: Format) -> Result<()> {
        if self.has_conditions() {
            body.resize(body.len() + self.width, 0);
            return Ok(());
        }
        for node in &self.nodes {
            for _ in 0..node.repeat.as_ref().map_or(1, |r| r.max) {
                node.element.defaults(body, format)?;
            }
        }
        Ok(())
    }
}

impl Count {
    fn compile(repeat: &Repeat, scopes: &[Symbols]) -> Result<Self> {
        match repeat {
            Repeat::Fixed(n) if *n > 0 => Ok(Self {
                min: *n,
                max: *n,
                source: None,
            }),
            Repeat::Fixed(_) => Err(Error::config("fixed repeat must be a positive integer")),
            Repeat::Variable(r) => {
                if r.max == 0 || r.min > r.max {
                    return Err(Error::config("repeat requires 0 <= min <= max and max > 0"));
                }
                if !identifier(&r.count_field) {
                    return Err(Error::config("repeat.count_field must be a field name"));
                }
                let (depth, t) = scopes.iter().enumerate().rev().find_map(|(depth, scope)| scope.get(&r.count_field).map(|t| (depth, t)))
                    .ok_or_else(|| Error::config(format!("repeat.count_field '{}' must reference a preceding field in the same or an enclosing group", r.count_field)))?;
                if !counter_type(t) {
                    return Err(Error::config(
                        "repeat.count_field requires an integer or scale-0 decimal field",
                    ));
                }
                Ok(Self {
                    min: r.min,
                    max: r.max,
                    source: Some((depth, r.count_field.clone())),
                })
            }
        }
    }

    fn value(&self, scopes: &[Row], output: bool) -> Result<usize> {
        let Some((depth, name)) = &self.source else {
            return Ok(self.max);
        };
        self.value_from(scopes.get(*depth).and_then(|s| s.get(name)), output)
    }

    fn value_from(&self, value: Option<&ScalarValue>, output: bool) -> Result<usize> {
        let Some((_, name)) = &self.source else {
            return Ok(self.max);
        };
        let invalid = || {
            Error::new(
                if output { 5 } else { 3 },
                format!(
                    "repeat.count_field '{name}' must be non-NULL and an integer in {}..={}",
                    self.min, self.max
                ),
            )
        };
        let value = value.ok_or_else(invalid)?;
        let n: i128 = match value {
            ScalarValue::Int8(Some(n)) => (*n).into(),
            ScalarValue::Int16(Some(n)) => (*n).into(),
            ScalarValue::Int32(Some(n)) => (*n).into(),
            ScalarValue::Int64(Some(n)) => (*n).into(),
            ScalarValue::UInt8(Some(n)) => (*n).into(),
            ScalarValue::UInt16(Some(n)) => (*n).into(),
            ScalarValue::UInt32(Some(n)) => (*n).into(),
            ScalarValue::UInt64(Some(n)) => (*n).into(),
            ScalarValue::Decimal128(Some(n), _, scale) => {
                crate::field::rescale(*n, (*scale).into(), 0).map_err(|_| invalid())?
            }
            _ => return Err(invalid()),
        };
        usize::try_from(n)
            .ok()
            .filter(|n| (self.min..=self.max).contains(n))
            .ok_or_else(invalid)
    }
}

fn leaf_columns(f: &CompiledField) -> Fields {
    f.names()
        .into_iter()
        .map(|name| ArrowField::new(name, f.data_type(), f.spec.nullable.unwrap_or(false)))
        .collect::<Vec<_>>()
        .into()
}

fn list_item(t: &DataType) -> Option<&Arc<ArrowField>> {
    match t {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => Some(f),
        _ => None,
    }
}

fn validate_leaf(f: &CompiledField, actual: &Fields) -> Result<()> {
    for name in f.names() {
        let column = actual
            .iter()
            .find(|c| c.name() == &name)
            .ok_or_else(|| Error::config(format!("missing output field '{name}'")))?;
        if !super::output::compatible(f, column.data_type()) {
            return Err(Error::config(format!(
                "output field '{name}' has incompatible type {:?}; use SQL CAST",
                column.data_type()
            )));
        }
    }
    Ok(())
}

fn struct_row(value: &ScalarValue) -> Result<Row> {
    let ScalarValue::Struct(array) = value else {
        return Err(Error::output("group requires a struct"));
    };
    if array.is_null(0) {
        return Err(Error::output(
            "NULL group cannot be written to a physical record",
        ));
    }
    array
        .fields()
        .iter()
        .zip(array.columns())
        .map(|(field, column)| {
            Ok((
                field.name().clone(),
                ScalarValue::try_from_array(column, 0).map_err(|e| Error::output(e.to_string()))?,
            ))
        })
        .collect()
}

fn make_struct(fields: &Fields, values: Vec<ScalarValue>) -> Result<ScalarValue> {
    let arrays = values
        .iter()
        .map(|v| {
            v.to_array_of_size(1)
                .map_err(|e| Error::input(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    // A group containing only FILLER still has one valid (empty) struct.
    let array = if fields.is_empty() {
        StructArray::new_empty_fields(1, None)
    } else {
        StructArray::try_new(fields.clone(), arrays, None)
            .map_err(|e| Error::input(e.to_string()))?
    };
    Ok(ScalarValue::Struct(Arc::new(array)))
}

fn charge(budget: &mut usize, bytes: usize) -> Result<()> {
    *budget = budget
        .checked_add(bytes)
        .filter(|n| *n <= MAX_RECORD)
        .ok_or_else(|| Error::input("decoded repeat record exceeds 16 MiB"))?;
    Ok(())
}

impl Element {
    fn rest(&self) -> bool {
        match self {
            Self::Leaf(f) => f.length.is_none(),
            Self::Group(g) => g.rest,
        }
    }

    fn width(&self) -> usize {
        match self {
            Self::Leaf(f) => f.length.unwrap_or(0),
            Self::Group(g) => g.width,
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::Leaf(f) if f.kind == FieldType::Bits => DataType::Struct(leaf_columns(f)),
            Self::Leaf(f) => f.data_type(),
            Self::Group(g) => DataType::Struct(g.columns.clone()),
        }
    }

    fn validate_type(&self, t: &DataType) -> Result<()> {
        match self {
            Self::Group(g) => {
                let DataType::Struct(fields) = t else {
                    return Err(Error::config("group output requires a struct"));
                };
                g.validate_output(fields)
            }
            Self::Leaf(f) if f.kind == FieldType::Bits => {
                let DataType::Struct(fields) = t else {
                    return Err(Error::config("repeated bits require an array of structs"));
                };
                if fields.len() != f.names().len() {
                    return Err(Error::config(
                        "repeated bits must match the named bit fields",
                    ));
                }
                validate_leaf(f, fields)
            }
            Self::Leaf(f) if super::output::compatible(f, t) => Ok(()),
            Self::Leaf(_) => Err(Error::config(format!(
                "incompatible repeated element type {t:?}; use SQL CAST"
            ))),
        }
    }

    fn encode_value(
        &self,
        value: &ScalarValue,
        body: &mut Vec<u8>,
        format: Format,
        scopes: &mut Vec<Row>,
    ) -> Result<bool> {
        match self {
            Self::Group(g) => g.encode(&struct_row(value)?, body, format, scopes),
            Self::Leaf(f) => {
                let values = if f.kind == FieldType::Bits {
                    let row = struct_row(value)?;
                    f.names()
                        .iter()
                        .map(|name| {
                            row.get(name)
                                .cloned()
                                .ok_or_else(|| Error::output(format!("missing bit '{name}'")))
                        })
                        .collect::<Result<Vec<_>>>()?
                } else {
                    vec![value.clone()]
                };
                body.extend(f.encode(&values, format)?);
                Ok(f.length.is_none())
            }
        }
    }

    fn defaults(&self, body: &mut Vec<u8>, format: Format) -> Result<()> {
        match self {
            Self::Group(g) => g.defaults(body, format),
            Self::Leaf(f) => {
                body.extend(f.encode(&vec![f.null(); f.names().len()], format)?);
                Ok(())
            }
        }
    }
}

impl Node {
    fn encode(
        &self,
        row: &Row,
        body: &mut Vec<u8>,
        format: Format,
        scopes: &mut Vec<Row>,
    ) -> Result<bool> {
        let Some(repeat) = &self.repeat else {
            return match &self.element {
                Element::Leaf(f) => {
                    let values = f
                        .names()
                        .iter()
                        .map(|name| {
                            row.get(name)
                                .cloned()
                                .ok_or_else(|| Error::output(format!("missing field '{name}'")))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    body.extend(f.encode(&values, format)?);
                    Ok(f.length.is_none())
                }
                Element::Group(_) => self.element.encode_value(
                    &row[self.name.as_ref().unwrap()],
                    body,
                    format,
                    scopes,
                ),
            };
        };
        let value = &row[self.name.as_ref().unwrap()];
        if value.is_null() {
            return Err(Error::output(
                "repeat array must not be NULL; use an empty array for zero elements",
            ));
        }
        let array: ArrayRef = match value {
            ScalarValue::List(a) => a.value(0),
            ScalarValue::LargeList(a) => a.value(0),
            ScalarValue::FixedSizeList(a) => a.value(0),
            _ => return Err(Error::output("repeat requires an array")),
        };
        let count = repeat.value(scopes, true)?;
        if array.len() != count {
            return Err(Error::output(format!(
                "repeat array length {} does not match required count {count}",
                array.len()
            )));
        }
        let mut rest = false;
        for i in 0..count {
            let value =
                ScalarValue::try_from_array(&array, i).map_err(|e| Error::output(e.to_string()))?;
            rest |= self
                .element
                .encode_value(&value, body, format, scopes)
                .map_err(|e| e.context(format!("element {}", i + 1)))?;
        }
        if format != Format::Rdw {
            for _ in count..repeat.max {
                self.element.defaults(body, format)?;
            }
        }
        Ok(rest)
    }
}

impl RepeatLayout {
    pub(crate) fn encode(
        &self,
        batch: &RecordBatch,
        index: usize,
        body: &mut Vec<u8>,
    ) -> Result<()> {
        let row: Row = batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, array)| {
                Ok((
                    field.name().clone(),
                    ScalarValue::try_from_array(array, index)
                        .map_err(|e| Error::output(e.to_string()))?,
                ))
            })
            .collect::<Result<_>>()?;
        body.clear();
        self.root.encode(&row, body, self.format, &mut Vec::new())?;
        if body.len() > MAX_RECORD {
            return Err(Error::output("repeat record exceeds 16 MiB"));
        }
        Ok(())
    }
}
