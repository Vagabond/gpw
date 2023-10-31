use anyhow::{anyhow, Result};
use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use clap::Parser;
use gpwgen::{
    args::{Args, Combine, Tessellate},
    generate::gen_to_disk,
    gpwascii::GpwAscii,
};
use hextree::{
    compaction::Compactor,
    h3ron::{FromH3Index, H3Cell},
    HexTreeMap,
};
use std::{
    fs::File,
    io::{BufReader, BufWriter, ErrorKind},
    path::PathBuf,
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
        .map(|src_path| -> Result<(File, File)> {
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
            Ok((src_file, dst_file))
        })
        .collect::<Result<Vec<(File, File)>>>()?;

    for (src_file, dst_file) in files {
        let mut rdr = BufReader::new(src_file);
        let mut dst = BufWriter::new(dst_file);
        let data = GpwAscii::parse(&mut rdr).unwrap();
        gen_to_disk(resolution, data, &mut dst)
    }

    Ok(())
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

    let mut map: HexTreeMap<f32, _> = HexTreeMap::with_compactor(SummationCompactor { resolution });

    for source in sources {
        let mut rdr = BufReader::new(source);
        loop {
            match (rdr.read_u64::<LE>(), rdr.read_f32::<LE>()) {
                (Ok(h3_index), Ok(val)) => {
                    let cell = H3Cell::from_h3index(h3_index);
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
        wtr.write_u64::<LE>(**cell)?;
        wtr.write_f32::<LE>(*val)?;
    }

    Ok(())
}

struct SummationCompactor {
    resolution: u8,
}

impl Compactor<f32> for SummationCompactor {
    fn compact(&mut self, res: u8, children: [Option<&f32>; 7]) -> Option<f32> {
        if res < self.resolution {
            return None;
        }
        if let [Some(v0), Some(v1), Some(v2), Some(v3), Some(v4), Some(v5), Some(v6)] = children {
            return Some(v0 + v1 + v2 + v3 + v4 + v5 + v6);
        };
        None
    }
}
