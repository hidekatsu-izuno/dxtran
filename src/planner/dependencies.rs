//! Resolve process dependencies in stable configuration order.
use super::sql::references;
use crate::{
    config::Job,
    error::{Error, Result},
};
use std::collections::{BTreeMap, BTreeSet};
pub(super) fn process_order(job: &Job) -> Result<Vec<usize>> {
    let input: BTreeSet<_> = job.input.iter().map(|d| d.id.clone()).collect();
    let process: BTreeMap<_, _> = job
        .process
        .iter()
        .enumerate()
        .map(|(i, p)| (p.id.clone(), i))
        .collect();
    let deps: Vec<_> = job
        .process
        .iter()
        .map(|p| {
            references(&p.sql).map_err(|e| {
                e.context(format!(
                    "process {} {}",
                    p.id,
                    p.origin
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default()
                ))
            })
        })
        .collect::<Result<_>>()?;
    for (id, refs) in job.process.iter().map(|p| &p.id).zip(&deps).chain(
        job.output.iter().map(|d| &d.id).zip(
            job.output
                .iter()
                .map(|d| {
                    references(d.sql.as_ref().unwrap()).map_err(|e| {
                        e.context(format!(
                            "output {} {}",
                            d.id,
                            d.sql_origin
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_default()
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?
                .iter(),
        ),
    ) {
        for r in refs {
            if !input.contains(r) && !process.contains_key(r) {
                return Err(Error::sql(format!("{id}: undefined table '{r}'")));
            }
        }
    }
    fn visit(
        i: usize,
        deps: &[BTreeSet<String>],
        process: &BTreeMap<String, usize>,
        state: &mut [u8],
        order: &mut Vec<usize>,
    ) -> Result<()> {
        if state[i] == 2 {
            return Ok(());
        }
        if state[i] == 1 {
            return Err(Error::sql("cyclic process dependency"));
        }
        state[i] = 1;
        // Walk dependencies in YAML order, independently of identifier spelling.
        for j in 0..state.len() {
            if process
                .iter()
                .any(|(name, index)| *index == j && deps[i].contains(name))
            {
                visit(j, deps, process, state, order)?;
            }
        }
        state[i] = 2;
        order.push(i);
        Ok(())
    }
    let mut state = vec![0; deps.len()];
    let mut order = vec![];
    for i in 0..state.len() {
        visit(i, &deps, &process, &mut state, &mut order)?;
    }
    Ok(order)
}
