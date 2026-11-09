//! Caller keys: `run --key K` and `ls --key-prefix P`.
//!
//! A key is how a caller that must survive its own crash finds the job it may
//! already have started. mu writes an idempotency key for every effect before
//! the effect, and after a crash it asks "does a job with my key exist?"
//! rather than starting a second one. The job id cannot serve: it is
//! generated, so it is unknown until the dispatch has been made.
//!
//! A key is **data, not a name** (SPEC § Let users choose job names): it is
//! stored as `$d/key` beside `cmd`, and never becomes a tmux session name, a
//! path component or a tmux target. The id stays generated, so the two
//! failures that rejected caller-chosen names -- a duplicate tmux name whose
//! wrapper still reported success, and a reused name inheriting a stale `rc`
//! -- cannot happen through a key.

/// Longest accepted key, in bytes. mu's keys are UUIDs (36); the room above
/// that is for a caller's own prefix.
pub const MAX_KEY_LEN: usize = 128;

/// A validated key: 1 to [`MAX_KEY_LEN`] bytes of `[A-Za-z0-9._-]`.
///
/// Validated at the boundary for the same reason [`crate::wrapper::JobId`]
/// is: a key is written into a remote shell script and matched by `grep`.
/// With no quote, space, `$`, glob or newline in the alphabet, the value is
/// inert in every layer it crosses and fits on one line of `$d/key`, so
/// `grep -Fx` matches it exactly. A leading `-` is refused too, so a key can
/// never be read as an option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobKey(String);

impl JobKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn check(raw: &str, what: &str) -> anyhow::Result<()> {
    let ok = !raw.is_empty()
        && raw.len() <= MAX_KEY_LEN
        && !raw.starts_with('-')
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if !ok {
        anyhow::bail!(
            "invalid {what} {raw:?}: expected 1 to {MAX_KEY_LEN} characters of A-Z a-z 0-9 . _ -, not starting with -"
        );
    }
    Ok(())
}

impl std::str::FromStr for JobKey {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        check(raw, "job key")?;
        Ok(Self(raw.to_string()))
    }
}

impl std::fmt::Display for JobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A validated `--key-prefix`: the same alphabet as a key.
///
/// The filter runs locally over a listing that already read every key, so
/// the alphabet is not needed for safety here; sharing it means a prefix that
/// parses can match some key that parses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPrefix(String);

impl KeyPrefix {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Does `key` start with this prefix? A job with no key never matches.
    pub fn matches(&self, key: Option<&str>) -> bool {
        key.is_some_and(|key| key.starts_with(&self.0))
    }
}

impl std::str::FromStr for KeyPrefix {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        check(raw, "key prefix")?;
        Ok(Self(raw.to_string()))
    }
}
