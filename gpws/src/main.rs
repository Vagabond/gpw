#![deny(clippy::unwrap_used)]

mod options;
use anyhow::Result;
use byteorder::{LittleEndian as LE, ReadBytesExt};
use clap::Parser;
use hextree::{Cell, HexTreeMap};
use hyper::{
    body::Body,
    service::{make_service_fn, service_fn},
    Error, Response, Server, StatusCode,
};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::{
    convert::TryFrom,
    fs::File,
    io::{BufReader, ErrorKind},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let args = options::Cli::parse();
    let f = File::open(args.path)?;
    let map: HexTreeMap<f32> = deserialize_hexmap(f)?;
    let map = Arc::new(map);

    let make_service = make_service_fn(move |_| {
        let map = map.clone();
        async move {
            Ok::<_, Error>(service_fn(move |req| {
                let path = req.uri().path();
                let population = u64::from_str_radix(&path[1..], 16)
                    .ok()
                    .and_then(|index| Cell::try_from(index).ok())
                    .and_then(|target_cell| match map.get(target_cell) {
                        Some((cell, _)) if target_cell.res() > cell.res() => None,
                        _ => Some(target_cell),
                    })
                    .map(|cell| map.subtree_iter(cell).map(|(_cell, pop)| pop).sum::<f32>());
                async move {
                    match population {
                        Some(pop) => {
                            Ok::<_, Error>(Response::new(Body::from(format!("{:?}", pop))))
                        }
                        None => {
                            let mut not_found = Response::default();
                            *not_found.status_mut() = StatusCode::NOT_FOUND;
                            Ok(not_found)
                        }
                    }
                }
            }))
        }
    });

    let addr = ([127, 0, 0, 1], 3000).into();
    let server = Server::bind(&addr).serve(make_service);

    println!("Listening on http://{}", addr);

    server.await?;
    Ok(())
}

fn deserialize_hexmap(src_file: File) -> Result<HexTreeMap<f32>> {
    let file_size = src_file.metadata()?.len();
    let mut map = HexTreeMap::new();
    let mut ret_err: Option<anyhow::Error> = None;
    let idx_val_pairs_total =
        file_size / (std::mem::size_of::<u64>() + std::mem::size_of::<f32>()) as u64;
    let idx_val_pairs_processed = AtomicU64::new(0);

    {
        println!("Deserializing hex map");
    }

    let hexmap_complete = AtomicBool::new(false);

    thread::scope(|s| {
        s.spawn(|| {
            let mut rdr = BufReader::new(src_file);
            loop {
                match (rdr.read_u64::<LE>(), rdr.read_f32::<LE>()) {
                    (Ok(h3_index), Ok(val)) => {
                        let cell = Cell::try_from(h3_index)
                            .expect("serialized hexmap should only contain valid indices");
                        map.insert(cell, val);
                        idx_val_pairs_processed.fetch_add(1, Ordering::Relaxed);
                    }
                    (Err(e), _) if e.kind() == ErrorKind::UnexpectedEof => {
                        hexmap_complete.store(true, Ordering::Relaxed);
                        break;
                    }
                    (Err(e), _) => {
                        ret_err = Some(e.into());
                        hexmap_complete.store(true, Ordering::Relaxed);
                        break;
                    }
                    (_, Err(e)) => {
                        ret_err = Some(e.into());
                        hexmap_complete.store(true, Ordering::Relaxed);
                        break;
                    }
                };
            }
        });

        s.spawn(|| {
            let pb = ProgressBar::new(idx_val_pairs_total);
            pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {human_pos}/{human_len} ({eta})")
                         .expect("incorrect progress bar format string")
                         .with_key("eta", |state: &ProgressState, w: &mut dyn std::fmt::Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
                         .progress_chars("#>-"));

            while !hexmap_complete.load(Ordering::Relaxed) && idx_val_pairs_processed.load(Ordering::Relaxed) < idx_val_pairs_total {
                pb.set_position(idx_val_pairs_processed.load(Ordering::Relaxed));
                thread::sleep(Duration::from_millis(12));
            }

            pb.finish_with_message("done");
        });
    });

    if let Some(err) = ret_err {
        Err(err)
    } else {
        Ok(map)
    }
}
