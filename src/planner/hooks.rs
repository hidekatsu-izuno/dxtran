use super::references;
use crate::{
    config::{Check, Job},
    error::{Error, Result},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(super) struct Selection {
    pub reachable: BTreeSet<String>,
    pub freeze: BTreeSet<String>,
    pub before_targets: BTreeMap<String, Vec<String>>,
}
impl Selection {
    pub fn new(job: &Job) -> Result<Self> {
        let mut selected = Self::default();
        for d in &job.input {
            if !d.before.is_empty() {
                return Err(Error::config("input supports after, not before").context(&d.id));
            }
        }
        for d in &job.output {
            if !d.after.is_empty() {
                return Err(Error::config("output supports before, not after").context(&d.id));
            }
        }
        if job.input.iter().all(|d| d.after.is_empty())
            && job.output.iter().all(|d| d.before.is_empty())
            && job
                .process
                .iter()
                .all(|p| p.before.is_empty() && p.after.is_empty())
        {
            return Ok(selected);
        }
        let dependencies = job
            .process
            .iter()
            .map(|p| Ok((p.id.clone(), references(&p.sql)?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut pending = job
            .output
            .iter()
            .map(|d| references(d.sql.as_ref().unwrap()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        while let Some(id) = pending.pop() {
            if selected.reachable.insert(id.clone())
                && let Some(deps) = dependencies.get(&id)
            {
                pending.extend(deps.iter().cloned());
            }
        }
        for d in &job.input {
            if !d.after.is_empty() {
                selected.freeze.insert(d.id.clone());
            }
        }
        for p in &job.process {
            let targets = p
                .before
                .iter()
                .map(|check| target(check, &dependencies[&p.id], &p.id))
                .collect::<Result<Vec<_>>>()?;
            if selected.reachable.contains(&p.id) {
                if !p.before.is_empty() || !p.after.is_empty() {
                    selected.freeze.insert(p.id.clone());
                }
                selected.freeze.extend(targets.iter().cloned());
            }
            selected.before_targets.insert(p.id.clone(), targets);
        }
        Ok(selected)
    }
}
fn target(check: &Check, refs: &BTreeSet<String>, id: &str) -> Result<String> {
    let target = if let Some(target) = &check.from {
        target.clone()
    } else if refs.len() == 1 {
        refs.first().unwrap().clone()
    } else {
        return Err(Error::config(
            "process.before requires from when SQL has zero or multiple sources",
        )
        .context(id));
    };
    if !refs.contains(&target) {
        return Err(Error::config(format!(
            "process.before from '{target}' must name a direct SQL dependency"
        ))
        .context(id));
    }
    Ok(target)
}
