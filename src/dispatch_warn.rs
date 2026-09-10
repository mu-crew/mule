//! Advisory warnings for commands that can hide failure or never end.
//!
//! These never block dispatch: the command belongs to the caller, and a final
//! pipeline may be exactly what they meant. The tokenizer is only enough shell
//! to avoid warning on quoted text, nested pipelines, and pipelines that feed
//! another command. Uncertain forms get no warning rather than a noisy one.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchWarning {
    PipelineStatus,
    UnboundedLoop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ShellToken {
    Word(String, bool),
    Pipe,
    And,
    OtherBoundary,
    Semi,
    Open,
    Close,
}

pub(crate) fn dispatch_warnings(command: &str, has_runtime_cap: bool) -> Vec<DispatchWarning> {
    let tokens = shell_tokens(command);
    let mut warnings = Vec::new();
    let pipefail_at = tokens.windows(3).position(|window| {
        matches!(
            window,
            [
                ShellToken::Word(set, false),
                ShellToken::Word(option, false),
                ShellToken::Word(pipefail, false)
            ] if set == "set" && option == "-o" && pipefail == "pipefail"
        )
    });

    for (index, token) in tokens.iter().enumerate() {
        if token != &ShellToken::Pipe || nested_at(&tokens, index) {
            continue;
        }
        let Some(ShellToken::Word(program, false)) = tokens.get(index + 1) else {
            continue;
        };
        if program != "head" && program != "tail" {
            continue;
        }
        let boundary = tokens[index + 2..].iter().find(|token| {
            matches!(
                token,
                ShellToken::Pipe
                    | ShellToken::And
                    | ShellToken::OtherBoundary
                    | ShellToken::Semi
                    | ShellToken::Close
            )
        });
        if boundary.is_none() || matches!(boundary, Some(ShellToken::And | ShellToken::Semi)) {
            if pipefail_at.is_none_or(|position| position >= index) {
                warnings.push(DispatchWarning::PipelineStatus);
            }
            break;
        }
    }

    if !has_runtime_cap {
        for index in 0..tokens.len().saturating_sub(2) {
            let starts_command = index == 0
                || matches!(
                    tokens[index - 1],
                    ShellToken::And | ShellToken::Semi | ShellToken::Open
                );
            let loop_words = matches!(
                (&tokens[index], &tokens[index + 1]),
                (ShellToken::Word(a, false), ShellToken::Word(b, false))
                    if (a == "while" && (b == "true" || b == ":"))
                        || (a == "until" && b == "false")
            );
            if starts_command && loop_words && tokens[index + 2] == ShellToken::Semi {
                warnings.push(DispatchWarning::UnboundedLoop);
                break;
            }
        }
    }

    warnings
}

fn nested_at(tokens: &[ShellToken], end: usize) -> bool {
    tokens[..end]
        .iter()
        .fold(0_i32, |depth, token| match token {
            ShellToken::Open => depth + 1,
            ShellToken::Close => depth.saturating_sub(1),
            _ => depth,
        })
        > 0
}

fn shell_tokens(command: &str) -> Vec<ShellToken> {
    let mut tokens = Vec::new();
    let mut word = String::new();
    let mut quoted = false;
    let mut quote = None;
    let mut chars = command.chars().peekable();

    let finish_word = |tokens: &mut Vec<ShellToken>, word: &mut String, quoted: &mut bool| {
        if !word.is_empty() {
            tokens.push(ShellToken::Word(std::mem::take(word), *quoted));
            *quoted = false;
        }
    };

    while let Some(ch) = chars.next() {
        if let Some(mark) = quote {
            if ch == mark {
                quote = None;
            } else {
                word.push(ch);
            }
            quoted = true;
            continue;
        }
        match ch {
            '\'' | '"' => {
                quote = Some(ch);
                quoted = true;
            }
            ' ' | '\t' => finish_word(&mut tokens, &mut word, &mut quoted),
            '\n' | ';' => {
                finish_word(&mut tokens, &mut word, &mut quoted);
                tokens.push(ShellToken::Semi);
            }
            '|' => {
                finish_word(&mut tokens, &mut word, &mut quoted);
                tokens.push(ShellToken::Pipe);
            }
            '&' => {
                finish_word(&mut tokens, &mut word, &mut quoted);
                if chars.peek() == Some(&'&') {
                    chars.next();
                    tokens.push(ShellToken::And);
                } else {
                    tokens.push(ShellToken::OtherBoundary);
                }
            }
            '(' => {
                finish_word(&mut tokens, &mut word, &mut quoted);
                tokens.push(ShellToken::Open);
            }
            ')' => {
                finish_word(&mut tokens, &mut word, &mut quoted);
                tokens.push(ShellToken::Close);
            }
            _ => word.push(ch),
        }
    }
    finish_word(&mut tokens, &mut word, &mut quoted);
    tokens
}

#[cfg(test)]
mod tests {
    use super::{DispatchWarning, dispatch_warnings};

    #[test]
    fn warns_when_head_or_tail_hides_a_pipeline_status() {
        let observed = "export PATH=$HOME/.elan/bin:$PATH; git checkout -q mh9 && git reset -q --hard mh9 && git log --oneline -1 && lake build 2>&1|tail -1 && ./runner/check 2>&1|tail -18";
        assert_eq!(
            dispatch_warnings(observed, false),
            vec![DispatchWarning::PipelineStatus]
        );

        for command in [
            "build | tail -3",
            "build | head -1 && deploy",
            "build | tail -1; notify",
        ] {
            assert_eq!(
                dispatch_warnings(command, false),
                vec![DispatchWarning::PipelineStatus],
                "missed {command:?}"
            );
        }
    }

    #[test]
    fn pipe_warning_skips_handled_and_nonterminal_pipelines() {
        for command in [
            "set -o pipefail; build | tail -3 && deploy",
            "build | tail -3 | grep error",
            "build | tail -3 & wait",
            "(build | tail -3); echo status recorded elsewhere",
        ] {
            assert!(
                dispatch_warnings(command, false).is_empty(),
                "false positive for {command:?}"
            );
        }
        assert_eq!(
            dispatch_warnings("build | tail -3; set -o pipefail", false),
            vec![DispatchWarning::PipelineStatus],
            "pipefail only handles pipelines after it is set"
        );
    }

    #[test]
    fn warns_only_for_explicit_unbounded_loop_syntax_without_a_cap() {
        for command in [
            "while true; do poll; done",
            "while :\ndo poll\ndone",
            "until false; do poll; done",
        ] {
            assert_eq!(
                dispatch_warnings(command, false),
                vec![DispatchWarning::UnboundedLoop],
                "missed {command:?}"
            );
            assert!(
                dispatch_warnings(command, true).is_empty(),
                "--max-secs must suppress {command:?}"
            );
        }

        for command in [
            "printf '%s' 'while true; do poll; done'",
            "echo while true",
            "while ready; do poll; done",
        ] {
            assert!(
                dispatch_warnings(command, false).is_empty(),
                "false positive for {command:?}"
            );
        }
    }
}
