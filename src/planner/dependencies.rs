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
    let edges: Vec<Vec<usize>> = deps
        .iter()
        .map(|refs| {
            let mut indices: Vec<_> = refs
                .iter()
                .filter_map(|name| process.get(name).copied())
                .collect();
            indices.sort_unstable();
            indices
        })
        .collect();
    fn visit(
        i: usize,
        edges: &[Vec<usize>],
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
        for &j in &edges[i] {
            visit(j, edges, state, order)?;
        }
        state[i] = 2;
        order.push(i);
        Ok(())
    }
    let mut state = vec![0; deps.len()];
    let mut order = vec![];
    for i in 0..state.len() {
        visit(i, &edges, &mut state, &mut order)?;
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn dependencies_follow_yaml_order_and_scale_to_many_independent_processes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("job.json");
        let mut config = json!({
            "input":[{"id":"data","path":"in.csv","format":"csv","encoding":"utf-8","fields":[{"name":"n"}]}],
            "process":[
                {"id":"result","sql":"SELECT * FROM alpha UNION ALL SELECT * FROM zulu"},
                {"id":"zulu","sql":"SELECT * FROM data"},
                {"id":"alpha","sql":"SELECT * FROM data"}],
            "output":[{"id":"out","sql":"SELECT * FROM result","path":"out.csv","format":"csv","encoding":"utf-8"}]
        });
        std::fs::write(&path, config.to_string()).unwrap();
        assert_eq!(
            process_order(&crate::config::load(&path).unwrap()).unwrap(),
            vec![1, 2, 0]
        );
        config["process"] = (0..1000)
            .map(|i| json!({"id":format!("p{i}"),"sql":"SELECT * FROM data"}))
            .collect();
        config["output"][0]["sql"] = json!("SELECT * FROM p999");
        std::fs::write(&path, config.to_string()).unwrap();
        assert_eq!(
            process_order(&crate::config::load(&path).unwrap()).unwrap(),
            (0..1000).collect::<Vec<_>>()
        );
    }
}
