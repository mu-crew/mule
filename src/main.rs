use std::io::Write;

use clap::Parser;

fn main() {
    let cli = mule::cli::Cli::parse();
    match mule::cli::dispatch(cli) {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            // `{:#}` so the whole context chain surfaces; the outermost frame
            // alone routinely hides the actual cause.
            // `writeln!`, not `eprintln!`: after a hangup stderr can be a dead
            // tty or a closed pipe, and `eprintln!` panics on EIO/EPIPE,
            // replacing the exit code with 101.
            let _ = writeln!(std::io::stderr(), "mule: {e:#}");
            std::process::exit(mule::errors::exit_code(&e));
        }
    }
}
