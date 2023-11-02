use clap::Parser;
use h3o::Resolution;

#[derive(Parser, Debug)]
pub enum Args {
    Tessellate(Tessellate),
    Combine(Combine),
}

/// Tessellate global world population (GPW) asc file grids into H3
/// cell/value pairs.
#[derive(Parser, Debug)]
pub struct Tessellate {
    /// Intermediate H3 resolution.
    #[arg(short, long, default_value_t = Resolution::Ten)]
    pub resolution: Resolution,
    /// Input GPW ASCII file.
    pub sources: Vec<std::path::PathBuf>,
    /// Output directory.
    #[arg(short, long)]
    pub outdir: std::path::PathBuf,
}

/// Combine multiple h3tess files into a single serialized H3 map at
/// the specified resolution.
#[derive(Parser, Debug)]
pub struct Combine {
    /// H3 resolution.
    #[arg(short, long, default_value_t = Resolution::Eight)]
    pub resolution: Resolution,
    /// h3tess source files.
    pub sources: Vec<std::path::PathBuf>,
    /// Output file.
    #[arg(short, long)]
    pub output: std::path::PathBuf,
}
