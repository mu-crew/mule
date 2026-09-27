use clap::Parser;

fn main() {
    let cli = mule::cli::Cli::parse();
    match mule::cli::dispatch(cli) {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            // `{:#}` so the whole context chain surfaces; the outermost frame
            // alone routinely hides the actual cause.
            eprintln!("mule: {e:#}");
            std::process::exit(mule::errors::exit_code(&e));
        }
    }
}
