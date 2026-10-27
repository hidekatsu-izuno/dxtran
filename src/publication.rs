//! Validate destinations, prepare access rights for every output, then publish.
use crate::filesystem::{Security, filesystem, identity};
use crate::{
    config::Job,
    error::{Error, Result},
};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    path::{Path, PathBuf},
};

fn canonical_destination(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent)
        .map_err(|e| Error::config(e.to_string()).context(parent.display()))?;
    if !parent.is_dir() {
        return Err(Error::config("output parent must be an existing directory"));
    }
    Ok(parent.join(
        path.file_name()
            .ok_or_else(|| Error::config("invalid output filename"))?,
    ))
}
fn ancestor(path: &Path) -> Result<PathBuf> {
    let mut p = path;
    loop {
        if p.exists() {
            return fs::canonicalize(p).map_err(|e| Error::config(e.to_string()));
        }
        p = p
            .parent()
            .ok_or_else(|| Error::config("temporary directory has no existing ancestor"))?;
    }
}
pub fn preflight(job: &Job, temp: &Path) -> Result<()> {
    let temporary = ancestor(temp)?;
    if !temporary.is_dir() {
        return Err(Error::config("temporary directory must be a directory"));
    }
    let fs = filesystem(&temporary).map_err(|e| Error::config(e.to_string()))?;
    let mut paths = BTreeSet::new();
    let mut inodes = BTreeSet::new();
    for input in &job.input {
        let path = fs::canonicalize(&input.path)
            .map_err(|e| Error::config(e.to_string()).context(input.path.display()))?;
        if !path.is_file() {
            return Err(Error::config("input must be a regular file"));
        }
        paths.insert(path);
        inodes.insert(identity(&input.path).map_err(|e| Error::config(e.to_string()))?);
    }
    for output in &job.output {
        let dest = canonical_destination(&output.path)?;
        let existing = match fs::symlink_metadata(&dest) {
            Ok(meta) if meta.file_type().is_file() => true,
            Ok(_) => return Err(Error::config("output must be a regular file")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(Error::config(e.to_string())),
        };
        let resolved = if existing {
            fs::canonicalize(&dest).map_err(|e| Error::config(e.to_string()))?
        } else {
            dest.clone()
        };
        let duplicate_inode =
            existing && !inodes.insert(identity(&dest).map_err(|e| Error::config(e.to_string()))?);
        if !paths.insert(resolved) || duplicate_inode {
            return Err(Error::config(format!(
                "input/output path collision: {}",
                dest.display()
            )));
        }
        if existing && output.mode.as_deref() != Some("overwrite") {
            return Err(Error::config(format!(
                "output already exists: {}",
                dest.display()
            )));
        }
        if filesystem(dest.parent().unwrap()).map_err(|e| Error::config(e.to_string()))? != fs {
            return Err(Error::config(
                "temporary directory and all output parents must be on the same filesystem and mount",
            ));
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct DestinationState {
    identity: (u64, u64),
    security: Security,
}
impl DestinationState {
    fn read(path: &Path) -> Result<Option<Self>> {
        match fs::symlink_metadata(path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::output(e.to_string())),
            Ok(meta) if !meta.file_type().is_file() => {
                return Err(Error::output("output must be a regular file, not a link"));
            }
            Ok(_) => (),
        }
        Ok(Some(Self {
            identity: identity(path).map_err(|e| Error::output(e.to_string()))?,
            security: Security::read(path).map_err(|e| Error::output(e.to_string()))?,
        }))
    }
}
struct PreparedOutput<'a> {
    source: &'a Path,
    destination: PathBuf,
    previous: Option<DestinationState>,
    // Windows excludes other opens until the prepared file has been renamed.
    file: File,
}
fn prepare_outputs<'a>(job: &Job, staged: &'a [PathBuf]) -> Result<Vec<PreparedOutput<'a>>> {
    if job.output.len() != staged.len() {
        return Err(Error::output("staged output count does not match the job"));
    }
    job.output
        .iter()
        .zip(staged)
        .map(|(output, source)| {
            let prepare = || -> Result<_> {
                let destination = canonical_destination(&output.path)?;
                let previous = DestinationState::read(&destination)?;
                if previous.is_some() && output.mode.as_deref() != Some("overwrite") {
                    return Err(Error::output("output already exists"));
                }
                let file =
                    crate::filesystem::prepare(source, previous.as_ref().map(|p| &p.security))
                        .map_err(|e| Error::output(e.to_string()))?;
                Ok(PreparedOutput {
                    source,
                    destination,
                    previous,
                    file,
                })
            };
            prepare().map_err(|e| {
                e.context(format!(
                    "output {}: permission preparation failed; no outputs published",
                    output.id
                ))
            })
        })
        .collect()
}
pub fn publish(job: &Job, staged: &[PathBuf]) -> Result<()> {
    let prepared = prepare_outputs(job, staged)?;
    publish_prepared(job, prepared)
}
fn publish_prepared(job: &Job, prepared: Vec<PreparedOutput<'_>>) -> Result<()> {
    let mut published = vec![];
    for (i, (output, prepared)) in job.output.iter().zip(prepared).enumerate() {
        let publish = || -> Result<()> {
            if DestinationState::read(&prepared.destination)? != prepared.previous {
                return Err(Error::output(
                    "destination or access rights changed before publication",
                ));
            }
            // An initially missing destination must not overwrite a racing creator.
            crate::filesystem::publish(
                prepared.source,
                &prepared.destination,
                prepared.previous.is_some(),
                &prepared.file,
            )
            .map_err(|e| Error::output(e.to_string()))
        };
        if let Err(e) = publish() {
            return Err(e.context(format!(
                "publication failed; published={published:?}; unpublished={:?}",
                job.output[i..]
                    .iter()
                    .map(|d| (&d.id, &d.path))
                    .collect::<Vec<_>>()
            )));
        }
        published.push((&output.id, &output.path));
        if let Err(e) = crate::filesystem::sync_directory(prepared.source.parent().unwrap())
            .and_then(|_| crate::filesystem::sync_directory(prepared.destination.parent().unwrap()))
        {
            return Err(Error::output(e.to_string()).context(format!(
                "publication sync failed; published={published:?}; unpublished={:?}",
                job.output[i + 1..]
                    .iter()
                    .map(|d| (&d.id, &d.path))
                    .collect::<Vec<_>>()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn two_outputs(dir: &Path) -> Job {
        let config = serde_json::json!({"input":[{"id":"data","format":"csv","encoding":"utf-8","path":"in.csv","fields":[{"name":"value"}]}],"output":[{"id":"first","format":"csv","encoding":"utf-8","sql":"SELECT value FROM data","path":"first.csv","mode":"overwrite"},{"id":"second","format":"csv","encoding":"utf-8","sql":"SELECT value FROM data","path":"second.csv","mode":"overwrite"}]});
        let jobfile = dir.join("job.yml");
        fs::write(&jobfile, config.to_string()).unwrap();
        crate::config::load(&jobfile).unwrap()
    }
    #[test]
    fn permission_preparation_failure_preserves_every_destination() {
        let dir = tempfile::tempdir().unwrap();
        let job = two_outputs(dir.path());
        let first = dir.path().join("first.csv");
        fs::write(&first, b"old").unwrap();
        let security = Security::read(&first).unwrap();
        fs::create_dir(dir.path().join("second.csv")).unwrap();
        let staged = [dir.path().join("a.tmp"), dir.path().join("b.tmp")];
        for path in &staged {
            fs::write(path, b"new").unwrap();
        }
        let error = publish(&job, &staged).unwrap_err();
        assert_eq!(error.code, 5);
        assert!(error.message.contains("no outputs published"));
        assert_eq!(fs::read(&first).unwrap(), b"old");
        assert_eq!(Security::read(&first).unwrap(), security);
        assert!(staged.iter().all(|p| p.exists()));
    }

    #[test]
    fn publication_failure_reports_exact_partial_state() {
        let dir = tempfile::tempdir().unwrap();
        let job = two_outputs(dir.path());
        let staged = [dir.path().join("a.tmp"), dir.path().join("b.tmp")];
        for path in &staged {
            fs::write(path, b"new").unwrap();
        }
        let prepared = prepare_outputs(&job, &staged).unwrap();
        // An initially absent destination appears after preparation.
        fs::write(&job.output[1].path, b"racing writer").unwrap();
        let error = publish_prepared(&job, prepared).unwrap_err();
        assert_eq!(error.code, 5);
        assert!(error.message.contains("published=[(\"first\""));
        assert!(error.message.contains("unpublished=[(\"second\""));
        assert_eq!(fs::read(&job.output[0].path).unwrap(), b"new");
        assert_eq!(fs::read(&job.output[1].path).unwrap(), b"racing writer");
        assert!(!staged[0].exists());
        assert!(staged[1].exists());
    }
}
