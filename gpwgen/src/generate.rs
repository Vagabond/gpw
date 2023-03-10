use crate::gpwascii::{GpwAscii, GpwAsciiHeader};
use geo::{coord, line_string, Polygon};
use hextree::h3ron;
use rayon::prelude::*;
use std::io::Write;

pub fn tessalate_grid(header: &GpwAsciiHeader, row: usize, col: usize) -> Vec<u64> {
    let grid_bottom_degs = header.yllcorner + header.cellsize * (header.nrows - row - 1) as f64;
    let grid_top_degs = grid_bottom_degs + header.cellsize;
    let grid_left_degs = header.xllcorner + header.cellsize * col as f64;
    let grid_right_degs = grid_left_degs + header.cellsize;

    let grid_cell_poly = Polygon::new(
        line_string![
            // lower-left
            coord! {x: grid_left_degs, y: grid_bottom_degs},
            // lower-right
            coord! {x: grid_right_degs, y: grid_bottom_degs},
            // upper-right
            coord! {x: grid_right_degs, y: grid_top_degs},
            // upper-left
            coord! {x: grid_left_degs, y: grid_top_degs},
            // lower-left
            coord! {x: grid_left_degs, y: grid_bottom_degs}
        ],
        vec![],
    );
    // Tesselate at res 10 so we can handle the two coordinate systems
    // drifting.
    let hexes = h3ron::polygon_to_cells(&grid_cell_poly, 10).unwrap();
    hexes.iter().map(|hex| *hex).collect()
}

pub fn gen_to_disk(src: GpwAscii, dst: &mut impl Write) {
    let (tx, rx) = std::sync::mpsc::channel::<(Vec<u64>, f32)>();

    let handle = std::thread::spawn(move || {
        let header = &src.header;
        let data = &src.data;
        data.into_par_iter()
            .enumerate()
            .for_each_with(tx, |tx, (row_idx, row)| {
                row.par_iter()
                    .enumerate()
                    .for_each_with(tx.clone(), |tx, (col_idx, sample)| {
                        if let Some(val) = sample {
                            let h3_indicies = tessalate_grid(header, row_idx, col_idx);
                            tx.send((h3_indicies, *val)).unwrap();
                        }
                    })
            })
    });

    while let Ok((h3_indicies, val)) = rx.recv() {
        let scaled_val = val / h3_indicies.len() as f32;
        let scaled_val_bytes = scaled_val.to_le_bytes();
        for h3_index in h3_indicies {
            dst.write_all(&h3_index.to_le_bytes()).unwrap();
            dst.write_all(&scaled_val_bytes).unwrap();
        }
    }
    handle.join().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::File,
        io::{BufReader, BufWriter, Cursor},
    };

    #[test]
    fn test_parse_header() {
        let header = r#"ncols         10800
nrows         10800
xllcorner     -180
yllcorner     -4.2632564145606e-14
cellsize      0.0083333333333333
NODATA_value  -9999
"#;
        let mut rdr = BufReader::new(Cursor::new(header));
        GpwAsciiHeader::parse(&mut rdr).unwrap();
    }

    #[test]
    fn test_parse() {
        let file = r#"ncols         4
nrows         4
xllcorner     -180
yllcorner     -4.2632564145606e-14
cellsize      0.0083333333333333
NODATA_value  -9999
-9999 -9999 -9999 -9999
-9999 -9999 -9999 -9999
-9999 -9999 -9999 -9999
-9999 -9999 0.123 -9999
"#;
        let mut rdr = BufReader::new(Cursor::new(file));
        GpwAscii::parse(&mut rdr).unwrap();
    }

    #[test]
    fn test_gen_to_disk() {
        let file = r#"ncols         4
nrows         4
xllcorner     -180
yllcorner     -4.2632564145606e-14
cellsize      0.0083333333333333
NODATA_value  -9999
-9999 -9999 -9999 -9999
-9999 -9999 -9999 -9999
-9999 -9999 -9999 -9999
-9999 -9999 0.123 -9999
"#;
        let mut rdr = BufReader::new(Cursor::new(file));
        let data = GpwAscii::parse(&mut rdr).unwrap();
        let mut dst = BufWriter::new(File::create("/Users/jay/he/gpw/out.indicies").unwrap());
        gen_to_disk(data, &mut dst);
    }
}
