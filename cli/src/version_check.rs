//! version_check — semver parsing + support-window arithmetic for the
//! CLI↔cloud compatibility check. Pure: no I/O, no network.
//!
//! Policy: a `basin` CLI on minor N is supported against a Basin Cloud
//! running minor N-1, N, or N+1. Patch drift always supported.
//! Cross-major drift is always outside the window.

use crate::error::{msg, CliResult};

/// Semver carries the three numeric components of "M.m.p". Pre-release +
/// build metadata are deliberately dropped — the window cares about
/// minor drift, not RC identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Semver {
    pub major: i64,
    pub minor: i64,
    pub patch: i64,
}

/// parse_semver accepts "1.2.3", "v1.2.3", "1.2.3-rc.4", "1.2.3+build5",
/// trimming surrounding whitespace. Errors when fewer than three numeric
/// components are present or any component isn't a non-negative integer.
pub fn parse_semver(s: &str) -> CliResult<Semver> {
    let mut s = s.trim();
    s = s.strip_prefix('v').unwrap_or(s);
    if let Some(i) = s.find(['-', '+']) {
        s = &s[..i];
    }
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() < 3 {
        return Err(msg("version: need at least major.minor.patch"));
    }
    let mut out = Semver {
        major: 0,
        minor: 0,
        patch: 0,
    };
    for (idx, dst) in [&mut out.major, &mut out.minor, &mut out.patch]
        .into_iter()
        .enumerate()
    {
        let n: i64 = parts[idx].parse().map_err(|_| {
            msg(format!(
                "version: component {} is not a non-negative integer",
                parts[idx]
            ))
        })?;
        if n < 0 {
            return Err(msg("version: components must be non-negative"));
        }
        *dst = n;
    }
    Ok(out)
}

impl std::fmt::Display for Semver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// in_support_window reports whether `cloud` sits inside `cli`'s
/// two-minor window. Different majors → never; |minor diff| > 1 →
/// outside; otherwise in (patch drift always fine).
pub fn in_support_window(cli: Semver, cloud: Semver) -> bool {
    if cli.major != cloud.major {
        return false;
    }
    (cli.minor - cloud.minor).abs() <= 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_and_prefixed() {
        assert_eq!(
            parse_semver("1.2.3").unwrap(),
            Semver {
                major: 1,
                minor: 2,
                patch: 3
            }
        );
        assert_eq!(
            parse_semver("v1.2.3").unwrap(),
            Semver {
                major: 1,
                minor: 2,
                patch: 3
            }
        );
        assert_eq!(
            parse_semver(" 1.2.3 ").unwrap(),
            Semver {
                major: 1,
                minor: 2,
                patch: 3
            }
        );
    }

    #[test]
    fn strips_prerelease_and_build() {
        assert_eq!(parse_semver("1.2.3-rc.4").unwrap().patch, 3);
        assert_eq!(parse_semver("1.2.3+build5").unwrap().patch, 3);
    }

    #[test]
    fn rejects_malformed() {
        assert!(parse_semver("1.2").is_err());
        assert!(parse_semver("1.x.3").is_err());
        assert!(parse_semver("").is_err());
    }

    #[test]
    fn window_arithmetic() {
        let cli = Semver {
            major: 1,
            minor: 5,
            patch: 0,
        };
        assert!(in_support_window(
            cli,
            Semver {
                major: 1,
                minor: 4,
                patch: 9
            }
        ));
        assert!(in_support_window(
            cli,
            Semver {
                major: 1,
                minor: 5,
                patch: 0
            }
        ));
        assert!(in_support_window(
            cli,
            Semver {
                major: 1,
                minor: 6,
                patch: 2
            }
        ));
        assert!(!in_support_window(
            cli,
            Semver {
                major: 1,
                minor: 7,
                patch: 0
            }
        ));
        assert!(!in_support_window(
            cli,
            Semver {
                major: 2,
                minor: 5,
                patch: 0
            }
        ));
    }
}
