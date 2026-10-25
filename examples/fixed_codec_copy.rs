//! Benchmark baseline using dxtran's field codecs without Arrow or DataFusion.
use dxtran::{
    config,
    error::{Error, Result},
    layout::RecordLayout,
    source::InputReader,
};
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::Path,
    time::Instant,
};

fn run(job_path: &Path, stage: &Path) -> Result<()> {
    let start = Instant::now();
    let job = config::load(job_path)?;
    let input = &job.input[0];
    let output = &job.output[0];
    let input_layout = RecordLayout::compile(input, false, None)?;
    let output_layout = RecordLayout::compile(output, true, Some(input_layout.schema.clone()))?;
    let columns = output_layout
        .fields
        .iter()
        .map(|f| {
            f.names()
                .iter()
                .map(|name| {
                    input_layout
                        .schema
                        .index_of(name)
                        .map_err(|e| Error::config(e.to_string()))
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let mut reader = InputReader::open(input, &input_layout)?;
    let mut writer = BufWriter::new(
        File::options()
            .write(true)
            .create_new(true)
            .open(stage)
            .map_err(|e| Error::output(e.to_string()))?,
    );
    let mut rows = 0u64;
    while let Some(row) = reader.next_row()? {
        let mut body = vec![0; output_layout.length];
        for (field, indices) in output_layout.fields.iter().zip(&columns) {
            let values = indices.iter().map(|i| row[*i].clone()).collect::<Vec<_>>();
            let bytes = field.encode(&values, output.format)?;
            body[field.offset..field.offset + bytes.len()].copy_from_slice(&bytes);
        }
        writer
            .write_all(&body)
            .map_err(|e| Error::output(e.to_string()))?;
        rows += 1;
    }
    writer
        .flush()
        .and_then(|_| writer.get_ref().sync_all())
        .map_err(|e| Error::output(e.to_string()))?;
    drop(writer);
    fs::rename(stage, &output.path)
        .and_then(|_| File::open(stage.parent().unwrap())?.sync_all())
        .and_then(|_| File::open(output.path.parent().unwrap())?.sync_all())
        .map_err(|e| Error::output(e.to_string()))?;
    println!(
        "{{\"seconds\":{},\"rows\":{rows},\"bytes\":{}}}",
        start.elapsed().as_secs_f64(),
        rows * output_layout.length as u64
    );
    Ok(())
}
fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 3 {
        eprintln!("usage: fixed_codec_copy JOB STAGE");
        std::process::exit(2);
    }
    if let Err(e) = run(Path::new(&args[1]), Path::new(&args[2])) {
        eprintln!("{e}");
        std::process::exit(i32::from(e.code));
    }
}
