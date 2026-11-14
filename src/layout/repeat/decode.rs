use super::*;

#[derive(Clone, Copy, Default)]
enum State {
    #[default]
    New,
    Reading,
    Done {
        start: usize,
        end: usize,
        active: bool,
        rest: bool,
    },
}

struct Frame<'a> {
    group: &'a Group,
    base: usize,
    limit: usize,
    chain: Vec<usize>,
    states: Vec<State>,
    values: Row,
    owners: BTreeMap<String, usize>,
}

struct Decoder<'a> {
    body: &'a [u8],
    format: Format,
    frames: Vec<Frame<'a>>,
    budget: usize,
}

impl<'a> Decoder<'a> {
    fn group(
        &mut self,
        group: &'a Group,
        base: usize,
        limit: usize,
        parent: Option<usize>,
    ) -> Result<(Vec<ScalarValue>, usize, bool)> {
        let index = self.frames.len();
        let mut chain = parent
            .map(|i| self.frames[i].chain.clone())
            .unwrap_or_default();
        chain.push(index);
        let limit = if let Some(length) = group
            .length
            .or((self.format != Format::Rdw).then_some(group.width))
        {
            base.checked_add(length)
                .filter(|end| *end <= limit)
                .ok_or_else(|| Error::input("group lies outside record body"))?
        } else {
            limit
        };
        let values = group
            .columns
            .iter()
            .map(|f| {
                Ok((
                    f.name().clone(),
                    ScalarValue::try_from(f.data_type())
                        .map_err(|e| Error::input(e.to_string()))?,
                ))
            })
            .collect::<Result<Row>>()?;
        self.frames.push(Frame {
            group,
            base,
            limit,
            chain,
            states: vec![State::New; group.nodes.len()],
            values,
            owners: BTreeMap::new(),
        });
        for i in 0..group.nodes.len() {
            self.node(index, i)?;
        }
        let mut ranges = Vec::new();
        for (i, state) in self.frames[index].states.iter().enumerate() {
            if let State::Done {
                start,
                end,
                active: true,
                rest,
            } = *state
            {
                ranges.push((start - base, end - base, i, rest));
            }
        }
        let (length, rest) = group.validate_ranges(&mut ranges, self.format, false)?;
        let end = length + base;
        let frame = self.frames.pop().unwrap();
        let values = group
            .columns
            .iter()
            .map(|f| frame.values[f.name()].clone())
            .collect();
        Ok((values, end, rest))
    }

    fn value(&mut self, frame: usize, name: &str, common: bool) -> Result<ScalarValue> {
        let group = self.frames[frame].group;
        for (i, node) in group.nodes.iter().enumerate() {
            if (!common || node.when_sql.is_none()) && node.columns.iter().any(|c| c.name() == name)
            {
                self.node(frame, i)?;
            }
        }
        self.frames[frame]
            .values
            .get(name)
            .cloned()
            .ok_or_else(|| Error::input(format!("missing field '{name}'")))
    }

    fn scoped_value(
        &mut self,
        frame: usize,
        depth: usize,
        name: &str,
        common: bool,
    ) -> Result<ScalarValue> {
        self.value(self.frames[frame].chain[depth], name, common)
    }

    fn start(&mut self, frame: usize, index: usize) -> Result<usize> {
        let node = &self.frames[frame].group.nodes[index];
        match node.position {
            Position::Absolute(offset) => Ok(self.frames[frame].base + offset),
            Position::After(previous) => {
                let prior = &self.frames[frame].group.nodes[previous];
                if prior.variable {
                    self.node(frame, previous)?;
                    let State::Done { end, .. } = self.frames[frame].states[previous] else {
                        unreachable!();
                    };
                    Ok(end)
                } else {
                    let width = prior.width;
                    Ok(self.start(frame, previous)? + width)
                }
            }
        }
    }

    fn node(&mut self, frame: usize, index: usize) -> Result<()> {
        match self.frames[frame].states[index] {
            State::Done { .. } => return Ok(()),
            State::Reading => {
                return Err(Error::input(
                    "cyclic dependency between when/count_field and physical field positions",
                ));
            }
            State::New => (),
        }
        self.frames[frame].states[index] = State::Reading;
        let node = &self.frames[frame].group.nodes[index];
        let result = (|| {
            let active = if let Some(condition) = &node.when {
                let values = condition
                    .bindings
                    .iter()
                    .map(|(depth, name)| self.scoped_value(frame, *depth, name, true))
                    .collect::<Result<Vec<_>>>()?;
                condition.evaluate(&values, false)?
            } else {
                true
            };
            let start = self.start(frame, index)?;
            if !active {
                let end = if node.variable {
                    start
                } else {
                    start + node.width
                };
                self.frames[frame].states[index] = State::Done {
                    start,
                    end,
                    active: false,
                    rest: false,
                };
                return Ok(());
            }
            let limit = self.frames[frame].limit;
            if start > limit {
                return Err(Error::input("field lies outside record body"));
            }
            let (values, end, rest) = if let Some(repeat) = &node.repeat {
                charge(&mut self.budget, size_of::<ScalarValue>())?;
                let count_value = repeat
                    .source
                    .as_ref()
                    .map(|(depth, name)| self.scoped_value(frame, *depth, name, false))
                    .transpose()?;
                let count = repeat.value_from(count_value.as_ref(), false)?;
                let mut position = start;
                let mut rest = false;
                let mut elements = Vec::new();
                for i in 0..count {
                    let (mut values, end, tail) = self
                        .element(&node.element, position, limit, frame)
                        .map_err(|e| e.context(format!("element {}", i + 1)))?;
                    let value = if let Element::Leaf(f) = &node.element
                        && f.kind == FieldType::Bits
                    {
                        make_struct(&leaf_columns(f), values)?
                    } else {
                        values.remove(0)
                    };
                    position = end;
                    rest |= tail;
                    elements.push(value);
                }
                if self.format != Format::Rdw {
                    position = start + node.width;
                }
                if position > limit {
                    return Err(Error::input(
                        "reserved repeat extent lies outside record body",
                    ));
                }
                let array = if elements.is_empty() {
                    new_empty_array(&node.element_type)
                } else {
                    ScalarValue::iter_to_array(elements).map_err(|e| Error::input(e.to_string()))?
                };
                let offsets = OffsetBuffer::new(
                    vec![
                        0,
                        i32::try_from(count)
                            .map_err(|_| Error::input("repeat count exceeds Arrow list limit"))?,
                    ]
                    .into(),
                );
                let list = ListArray::try_new(
                    Arc::new(ArrowField::new_list_field(
                        node.element_type.clone(),
                        node.element_nullable,
                    )),
                    offsets,
                    array,
                    None,
                )
                .map_err(|e| Error::input(e.to_string()))?;
                (vec![ScalarValue::List(Arc::new(list))], position, rest)
            } else {
                self.element(&node.element, start, limit, frame)?
            };
            for (field, value) in node.columns.iter().zip(values) {
                if self.frames[frame]
                    .owners
                    .insert(field.name().clone(), index)
                    .is_some()
                {
                    return Err(Error::input(format!(
                        "duplicate active field '{}'",
                        field.name()
                    )));
                }
                self.frames[frame]
                    .values
                    .insert(field.name().clone(), value);
            }
            self.frames[frame].states[index] = State::Done {
                start,
                end,
                active: true,
                rest,
            };
            Ok(())
        })();
        result.map_err(|e: Error| e.context(&node.origin))
    }

    fn element(
        &mut self,
        element: &'a Element,
        start: usize,
        limit: usize,
        parent: usize,
    ) -> Result<(Vec<ScalarValue>, usize, bool)> {
        match element {
            Element::Leaf(f) => {
                let end = f
                    .length
                    .map_or(Some(limit), |n| start.checked_add(n))
                    .filter(|end| *end <= limit)
                    .ok_or_else(|| Error::input("repeat field lies outside record body"))?;
                let bytes = self
                    .body
                    .get(start..end)
                    .ok_or_else(|| Error::input("repeat field lies outside record body"))?;
                let values = f.decode(bytes)?;
                for value in &values {
                    charge(&mut self.budget, value.size())?;
                }
                Ok((values, end, f.length.is_none()))
            }
            Element::Group(g) => {
                charge(&mut self.budget, size_of::<ScalarValue>())?;
                let (values, end, rest) = self.group(g, start, limit, Some(parent))?;
                Ok((vec![make_struct(&g.columns, values)?], end, rest))
            }
        }
    }
}

impl RepeatLayout {
    pub(crate) fn decode(&self, body: &[u8]) -> Result<Vec<ScalarValue>> {
        let mut decoder = Decoder {
            body,
            format: self.format,
            frames: Vec::new(),
            budget: 0,
        };
        let (values, end, _) = decoder.group(&self.root, 0, body.len(), None)?;
        if end != body.len() {
            return Err(Error::input(format!(
                "record body length {} does not match repeat layout length {end}",
                body.len()
            )));
        }
        Ok(values)
    }
}
