use clap::Parser;
use hextree::{h3ron::H3Cell, HexTreeMap};
use hyper::{
    body::Body,
    service::{make_service_fn, service_fn},
    Error, Response, Server, StatusCode,
};
use std::{convert::TryFrom, fs::File, io::BufReader, sync::Arc};

/// Search for a pattern in a file and display the lines that contain it.
#[derive(Parser)]
struct Cli {
    /// The path to the file to read
    #[clap(parse(from_os_str))]
    path: std::path::PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let f = BufReader::new(File::open(args.path).unwrap());
    let map: HexTreeMap<f64> = bincode::deserialize_from(f).unwrap();
    let map = Arc::new(map);

    #[forbid(clippy::unwrap_used)]
    let make_service = make_service_fn(move |_| {
        let map = map.clone();
        async move {
            Ok::<_, Error>(service_fn(move |req| {
                let path = req.uri().path();
                let population = u64::from_str_radix(&path[1..], 16)
                    .ok()
                    .and_then(|index| H3Cell::try_from(index).ok())
                    .and_then(|cell| {
                        map.reduce(cell, |_resolution, cells| {
                            cells.iter().sum::<f64>() / cells.len() as f64
                        })
                    });
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

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}
