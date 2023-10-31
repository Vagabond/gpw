use anyhow::{anyhow, Result};
use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use clap::Parser;
use gpwgen::{
    args::{Args, Combine, Tessellate},
    generate::gen_to_disk,
    gpwascii::GpwAscii,
};
use hextree::{compaction::Compactor, Cell, HexTreeMap};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::{
    fs::File,
    io::{BufReader, BufWriter, ErrorKind},
    path::{Path, PathBuf},
};
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<()> {
    let args = Args::parse();
    match args {
        Args::Tessellate(tess_args) => tessellate(tess_args)?,
        Args::Combine(combine_args) => combine(combine_args)?,
    };
    Ok(())
}

fn tessellate(
    Tessellate {
        resolution,
        sources,
        outdir,
    }: Tessellate,
) -> Result<()> {
    // Open all source and destination files at the same time,
    // otherwise fail fast.
    let files = sources
        .iter()
        .map(|src_path| -> Result<(&Path, File, File)> {
            let src_file = File::open(src_path)?;

            // Create the path to the output file with H3 resolution added and
            // gpwh3 extension.
            let dst_path = {
                let src_filename = src_path
                    .file_name()
                    .ok_or_else(|| anyhow!(format!("Not a file {:?}", src_path)))?;
                let mut dst = PathBuf::new();
                dst.push(&outdir);
                dst.push(src_filename);
                dst.set_extension(format!("res{}.h3tess", resolution));
                dst
            };
            let dst_file = File::create(dst_path)?;
            Ok((src_path, src_file, dst_file))
        })
        .collect::<Result<Vec<(&Path, File, File)>>>()?;

    for (n, (src_file_path, src_file, dst_file)) in files.iter().enumerate() {
        let mut rdr = BufReader::new(src_file);
        let mut dst = BufWriter::new(dst_file);
        let data = GpwAscii::parse(&mut rdr).unwrap();
        let n = n + 1;
        let m = files.len();
        let pb = make_progress_bar(src_file_path, n, m, data.len() as u64);
        gen_to_disk(resolution, data, pb, &mut dst)
    }

    Ok(())
}

/// Returns a progress bar object for the given parquet file and name.
fn make_progress_bar(path: &Path, n: usize, m: usize, total_cnt: u64) -> ProgressBar {
    #[allow(clippy::cast_sign_loss)]
    let pb = ProgressBar::new(total_cnt);
    pb.set_prefix(format!(
        "({n}/{m}) {}:\n",
        path.file_name().unwrap().to_string_lossy()
    ));
    pb.set_style(
        ProgressStyle::with_template("{prefix}[{wide_bar:.cyan/blue}]{eta_precise}")
            .expect("incorrect progress bar format string")
            .with_key(
                "eta",
                |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap();
                },
            )
            .progress_chars("#>-"),
    );
    pb
}

fn combine(
    Combine {
        resolution,
        sources,
        output,
    }: Combine,
) -> Result<()> {
    // Open all source files at the same time, otherwise fail fast.
    let sources = sources
        .iter()
        .map(File::open)
        .collect::<std::io::Result<Vec<File>>>()?;
    let output_file = File::create(output)?;

    let mut map: HexTreeMap<f32, _> = HexTreeMap::with_compactor(SummationCompactor {
        resolution: resolution.into(),
    });

    for source in sources {
        let mut rdr = BufReader::new(source);
        loop {
            match (rdr.read_u64::<LE>(), rdr.read_f32::<LE>()) {
                (Ok(h3_index), Ok(val)) => {
                    let cell = Cell::try_from(h3_index)?;
                    map.insert(cell, val)
                }
                (Err(e), _) if e.kind() == ErrorKind::UnexpectedEof => break,
                (err @ Err(_), _) => {
                    err?;
                }
                (_, err @ Err(_)) => {
                    err?;
                }
            };
        }
    }

    let mut wtr = BufWriter::new(output_file);
    for (cell, val) in map.iter() {
        wtr.write_u64::<LE>(cell.into_raw())?;
        wtr.write_f32::<LE>(*val)?;
    }

    Ok(())
}

struct SummationCompactor {
    resolution: u8,
}

impl Compactor<f32> for SummationCompactor {
    fn compact(&mut self, cell: Cell, children: [Option<&f32>; 7]) -> Option<f32> {
        if cell.res() < self.resolution {
            return None;
        }
        if let [Some(v0), Some(v1), Some(v2), Some(v3), Some(v4), Some(v5), Some(v6)] = children {
            return Some(v0 + v1 + v2 + v3 + v4 + v5 + v6);
        };
        None
    }
}
