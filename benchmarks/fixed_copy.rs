//! Standalone optimized baseline: explicit read/write, without kernel copy offload.
use std::{
    env,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
    time::Instant,
};

fn main() -> io::Result<()> {
    let args: Vec<_> = env::args_os().collect();
    if args.len() != 5 {
        return Err(io::Error::other(
            "usage: fixed-copy block|record INPUT OUTPUT STAGE",
        ));
    }
    let input = Path::new(&args[2]);
    let output = Path::new(&args[3]);
    let stage = Path::new(&args[4]);
    let start = Instant::now();
    let mut total = 0u64;
    match args[1].to_str().unwrap() {
        "block" => {
            let mut reader = File::open(input)?;
            let mut writer = File::options().write(true).create_new(true).open(stage)?;
            let mut buffer = vec![0; 1024 * 1024];
            loop {
                let count = reader.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                writer.write_all(&buffer[..count])?;
                total += count as u64;
            }
            writer.sync_all()?;
        }
        "record" => {
            let mut reader = BufReader::new(File::open(input)?);
            let mut writer =
                BufWriter::new(File::options().write(true).create_new(true).open(stage)?);
            let mut record = [0u8; 256];
            loop {
                match reader.read(&mut record[..1])? {
                    0 => break,
                    1 => reader.read_exact(&mut record[1..])?,
                    _ => unreachable!(),
                }
                writer.write_all(&record)?;
                total += record.len() as u64;
            }
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        _ => return Err(io::Error::other("unknown mode")),
    }
    fs::rename(stage, output)?;
    File::open(stage.parent().unwrap())?.sync_all()?;
    File::open(output.parent().unwrap())?.sync_all()?;
    println!(
        "{{\"seconds\":{},\"rows\":{},\"bytes\":{total}}}",
        start.elapsed().as_secs_f64(),
        total / 256
    );
    Ok(())
}
