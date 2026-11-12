//! Tracked files carry no term from the local term list (one term per line,
//! `#` comments) at `$MU_PUBLIC_TERMS`, else `~/.config/mu/public-terms.txt`.
//! Without that file the check passes.

use std::path::{Path, PathBuf};
use std::process::Command;

fn local_terms() -> Vec<String> {
    let file = std::env::var_os("MU_PUBLIC_TERMS")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|h| Path::new(&h).join(".config/mu/public-terms.txt"))
        });
    let Some(text) = file.and_then(|f| std::fs::read_to_string(f).ok()) else {
        return Vec::new();
    };
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_lowercase)
        .collect()
}

fn scan_text(text: &str, terms: &[String]) -> Vec<String> {
    let mut hits = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let lower = line.to_lowercase();
        for term in terms {
            if lower.contains(term.as_str()) {
                hits.push(format!("{}: term {term}", i + 1));
            }
        }
    }
    hits
}

#[test]
fn scan_text_flags_a_listed_term_case_insensitively() {
    assert_eq!(scan_text("uses ACME hosts", &["acme".to_string()]).len(), 1);
    assert!(scan_text("uses ACME hosts", &[]).is_empty());
}

#[test]
fn tracked_text_files_carry_no_listed_term() {
    let terms = local_terms();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let Ok(out) = Command::new("git")
        .args(["ls-files", "-z"])
        .current_dir(root)
        .output()
    else {
        return; // no git: nothing to guard
    };
    if terms.is_empty() || !out.status.success() {
        return; // no term list, or not a git checkout (a crate tarball)
    }
    let mut failures = Vec::new();
    for file in String::from_utf8_lossy(&out.stdout)
        .split('\0')
        .filter(|f| !f.is_empty())
    {
        let Ok(bytes) = std::fs::read(root.join(file)) else {
            continue; // deleted in the working tree
        };
        if bytes.contains(&0) {
            continue; // binary
        }
        for hit in scan_text(&String::from_utf8_lossy(&bytes), &terms) {
            failures.push(format!("{file}:{hit}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
