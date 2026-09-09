//! Job ids are parsed at the CLI boundary, so a hostile id cannot reach the
//! remote shell.

use coop::wrapper::JobId;

#[test]
fn hostile_ids_are_rejected() {
    // Every one of these was previously interpolated raw into a remote path, a
    // tmux target, and a shell script. `$(...)` in particular executed: a probe
    // built with the first of these produced
    //   d=$HOME/.local/state/coop/jobs/x$(touch /tmp/PWNED)y; ...
    // which is command execution on the remote host from a CLI argument.
    let hostile = [
        "x$(touch /tmp/PWNED)y",
        "abc123; rm -rf ~",
        "`id`",
        "../../etc/passwd",
        "$(id)",
        "a&&b",
        "a|b",
        "a b",
        "a'b",
        "a\"b",
        "a\nb",
        // tmux's target grammar reserves these two.
        "abc:12",
        "abc.12",
        // Wrong shape, so not an id coop ever minted.
        "",
        "abc12",
        "abc1234",
        "ABC123",
        "abcdefg",
        "zzzzzz",
    ];
    for raw in hostile {
        assert!(
            raw.parse::<JobId>().is_err(),
            "must reject hostile or malformed id {raw:?}"
        );
    }
}

#[test]
fn well_formed_ids_are_accepted_and_round_trip() {
    for raw in ["000000", "abc123", "ffffff", "0f1e2d"] {
        let id: JobId = raw.parse().unwrap();
        assert_eq!(id.as_str(), raw);
        assert_eq!(id.to_string(), raw);
    }
}

#[test]
fn generated_ids_always_parse() {
    // `run` mints ids and then parses them; if that ever failed it would be a
    // panic on the dispatch path rather than a rejected argument.
    for _ in 0..500 {
        let generated = coop::wrapper::new_id();
        assert!(
            generated.parse::<JobId>().is_ok(),
            "generated id {generated:?} does not parse"
        );
    }
}

#[test]
fn the_rejection_message_says_what_a_valid_id_looks_like() {
    let err = "nope".parse::<JobId>().unwrap_err().to_string();
    assert!(err.contains("hex"), "{err}");
    assert!(err.contains("coop run"), "{err}");
}
