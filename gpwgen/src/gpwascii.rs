use crate::error::GpwError;
use std::io::{BufRead, BufReader};

// $ head -n6    gpw_v4_population_count_rev11_2020_30_sec_1.asc
// ncols         10800
// nrows         10800
// xllcorner     -180
// yllcorner     -4.2632564145606e-14
// cellsize      0.0083333333333333
// NODATA_value  -9999
#[derive(Debug, Clone, PartialEq, Default)]
pub struct GpwAsciiHeader {
    pub ncols: usize,
    pub nrows: usize,
    pub xllcorner: f64,
    pub yllcorner: f64,
    pub cellsize: f64,
    pub nodata_value: String,
}

impl GpwAsciiHeader {
    pub fn parse<R: std::io::Read>(rdr: &mut BufReader<R>) -> Result<Self, GpwError> {
        let mut ncols: Option<usize> = None;
        let mut nrows: Option<usize> = None;
        let mut xllcorner: Option<f64> = None;
        let mut yllcorner: Option<f64> = None;
        let mut cellsize: Option<f64> = None;
        let mut nodata_value: Option<String> = None;

        for _ in 0..6 {
            let mut line = String::new();
            rdr.read_line(&mut line)?;
            let mut tokens = line.split_whitespace();
            if let Some(token) = tokens.next() {
                match token {
                    "ncols" => {
                        ncols = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("ncols", None))?
                                .parse::<usize>()
                                .map_err(|e| ("ncols", e))?,
                        );
                    }
                    "nrows" => {
                        nrows = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("nrows", None))?
                                .parse::<usize>()
                                .map_err(|e| ("ncols", e))?,
                        );
                    }
                    "xllcorner" => {
                        xllcorner = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("xllcorner", None))?
                                .parse::<f64>()
                                .map_err(|e| ("xllcorner", e))?,
                        );
                    }
                    "yllcorner" => {
                        yllcorner = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("yllcorner", None))?
                                .parse::<f64>()
                                .map_err(|e| ("yllcorner", e))?,
                        );
                    }
                    "cellsize" => {
                        cellsize = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("cellsize", None))?
                                .parse::<f64>()
                                .map_err(|e| ("cellsize", e))?,
                        );
                    }
                    "NODATA_value" => {
                        nodata_value = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("NODATA_value", None))?
                                .to_string(),
                        );
                    }
                    unexpected_token => {
                        Err(("unexpected header token", unexpected_token.to_string()))?
                    }
                }
            }
        }

        if let (
            Some(ncols),
            Some(nrows),
            Some(xllcorner),
            Some(yllcorner),
            Some(cellsize),
            Some(nodata_value),
        ) = (ncols, nrows, xllcorner, yllcorner, cellsize, nodata_value)
        {
            Ok(Self {
                ncols,
                nrows,
                xllcorner,
                yllcorner,
                cellsize,
                nodata_value,
            })
        } else {
            Err(GpwError::Parse("incomplete header", None))
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GpwAscii {
    pub header: GpwAsciiHeader,
    pub data: Vec<Vec<Option<f32>>>,
    pub filename: Option<String>,
}

impl GpwAscii {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.data
            .iter()
            .map(|row| row.iter().flatten().count())
            .sum()
    }

    pub fn parse<R: std::io::Read>(rdr: &mut BufReader<R>) -> Result<Self, GpwError> {
        let header = GpwAsciiHeader::parse(rdr)?;
        let mut data = Vec::with_capacity(header.nrows);
        let mut data_line = String::new();
        let mut row_idx = 0;
        while 0 != rdr.read_line(&mut data_line)? {
            let mut row = Vec::with_capacity(header.ncols);
            for (col_idx, cell) in data_line.split_whitespace().enumerate() {
                let sample = if cell == header.nodata_value {
                    None
                } else {
                    Some(cell.parse::<f32>().map_err(|e| {
                        (
                            "cell parse error",
                            format!("row {}, col {}, err {}", row_idx, col_idx, e),
                        )
                    })?)
                };
                row.push(sample);
            }
            assert_eq!(row.len(), header.ncols);
            row_idx += 1;
            data.push(row);
            data_line.clear();
        }
        assert_eq!(data.len(), header.nrows);
        Ok(Self {
            header,
            data,
            filename: None,
        })
    }
}
