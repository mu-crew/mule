use clap::Parser;

fn main() {
    let cli = coop::cli::Cli::parse();
    match coop::cli::dispatch(cli) {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            // `{:#}` so the whole context chain surfaces; the outermost frame
            // alone routinely hides the actual cause.
            eprintln!("coop: {e:#}");
            std::process::exit(1);
        }
    }
}
