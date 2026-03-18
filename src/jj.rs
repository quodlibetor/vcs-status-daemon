use anyhow::{Context, Result};
use std::path::Path;
use std::time::Duration;
use tokio::process::Command;

use crate::config::Config;
use crate::template::{Bookmark, RepoStatus};

const VCS_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

async fn run_jj(repo_path: &Path, args: &[&str]) -> Result<String> {
    let output = match tokio::time::timeout(
        VCS_COMMAND_TIMEOUT,
        Command::new("jj")
            .args(args)
            .current_dir(repo_path)
            .kill_on_drop(true)
            .output(),
    )
    .await
    {
        Ok(result) => result.context("failed to run jj")?,
        Err(_) => anyhow::bail!(
            "jj command timed out after {}s",
            VCS_COMMAND_TIMEOUT.as_secs()
        ),
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("jj command failed: {stderr}");
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Strip ANSI escape sequences from a string.
fn strip_ansi(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            while let Some(&next) = chars.peek() {
                chars.next();
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            result.push(c);
        }
    }
    result
}

/// Parse a `--stat` summary line like "3 files changed, 10 insertions(+), 5 deletions(-)"
pub(crate) fn parse_diff_stat(output: &str) -> (u32, u32, u32) {
    let mut files = 0u32;
    let mut added = 0u32;
    let mut removed = 0u32;
    for line in output.lines() {
        let line = line.trim();
        if line.contains("changed") {
            if let Some(n) = line.split_whitespace().next() {
                files = n.parse().unwrap_or(0);
            }
            if let Some(idx) = line.find("insertion") {
                let before = &line[..idx].trim();
                if let Some(n) = before.rsplit(", ").next().or(before.rsplit(' ').next()) {
                    added = n.trim().parse().unwrap_or(0);
                }
            }
            if let Some(idx) = line.find("deletion") {
                let before = &line[..idx].trim();
                if let Some(n) = before.rsplit(", ").next().or(before.rsplit(' ').next()) {
                    removed = n.trim().parse().unwrap_or(0);
                }
            }
        }
    }
    (files, added, removed)
}

pub async fn query_jj_status(
    repo_path: &Path,
    config: &Config,
    ignore_working_copy: bool,
) -> Result<RepoStatus> {
    let iwc: &[&str] = if ignore_working_copy {
        &["--ignore-working-copy"]
    } else {
        &[]
    };

    let repo_str = repo_path.to_string_lossy().to_string();

    let commit_template = r#"change_id.shortest(8) ++ "|||" ++ commit_id.shortest(8) ++ "|||" ++ description.first_line() ++ "|||" ++ conflict ++ "|||" ++ divergent ++ "|||" ++ hidden ++ "|||" ++ immutable ++ "|||" ++ empty"#;

    let bookmark_template = r#"bookmarks.map(|b| b.name()).join(" ") ++ "\n""#;
    let depth = config.bookmark_search_depth;

    let repo_str2 = repo_str.clone();
    let repo_str3 = repo_str.clone();

    let iwc_owned: Vec<String> = iwc.iter().map(|s| s.to_string()).collect();
    let iwc_owned2 = iwc_owned.clone();
    let iwc_owned3 = iwc_owned.clone();

    let color_flag = if config.color {
        "--color=always"
    } else {
        "--color=never"
    };

    let commit_fut = async {
        let mut args = vec!["log", "-r", "@", "--no-graph", color_flag, "-R", &repo_str];
        for a in &iwc_owned {
            args.push(a);
        }
        args.extend_from_slice(&["-T", commit_template]);
        run_jj(repo_path, &args).await
    };

    let bookmark_fut = async {
        let ancestor_expr = format!("ancestors(@, {depth})");
        let mut args = vec!["log", "-r", &ancestor_expr, "--no-graph", "-R", &repo_str2];
        for a in &iwc_owned2 {
            args.push(a);
        }
        args.extend_from_slice(&["-T", bookmark_template]);
        run_jj(repo_path, &args).await
    };

    let diff_fut = async {
        let mut args = vec!["diff", "-r", "@", "--stat", "-R", &repo_str3];
        for a in &iwc_owned3 {
            args.push(a);
        }
        run_jj(repo_path, &args).await
    };

    let (commit_out, bookmark_out, diff_out) =
        tokio::try_join!(commit_fut, bookmark_fut, diff_fut)?;

    let mut status = RepoStatus {
        is_jj: true,
        ..Default::default()
    };

    // Parse commit info
    let commit_line = commit_out.trim();
    let parts: Vec<&str> = commit_line.split("|||").collect();
    if parts.len() >= 8 {
        // change_id and commit_id keep their ANSI colors (from jj)
        status.change_id = parts[0].to_string();
        status.commit_id = parts[1].to_string();
        // Strip ANSI from fields we parse as text/booleans
        status.description = strip_ansi(parts[2]);
        status.conflict = strip_ansi(parts[3]) == "true";
        status.divergent = strip_ansi(parts[4]) == "true";
        status.hidden = strip_ansi(parts[5]) == "true";
        status.immutable = strip_ansi(parts[6]) == "true";
        status.empty = strip_ansi(parts[7]) == "true";
    }

    // Parse bookmarks - each line corresponds to an ancestor at distance i
    // Empty lines are significant (ancestor with no bookmarks), so don't skip them.
    // Deduplicate by name, keeping the closest (smallest distance) occurrence,
    // since DAG merges can cause the same bookmark to appear at multiple distances.
    let mut seen_bookmarks = std::collections::HashSet::new();
    for (distance, line) in bookmark_out.lines().enumerate() {
        let dist = distance as u32;
        for name in line.split_whitespace() {
            if !name.is_empty() && seen_bookmarks.insert(name.to_string()) {
                let display = if dist == 0 {
                    name.to_string()
                } else {
                    format!("{name}+{dist}")
                };
                status.bookmarks.push(Bookmark {
                    name: name.to_string(),
                    distance: dist,
                    display,
                });
            }
        }
    }

    let (f, a, r) = parse_diff_stat(&diff_out);
    status.files_changed = f;
    status.lines_added = a;
    status.lines_removed = r;

    // For jj, total = unstaged (no staging area)
    status.total_files_changed = f;
    status.total_lines_added = a;
    status.total_lines_removed = r;

    // Workspace name: read from .jj/working_copy/checkout
    // The file ends with a length-prefixed workspace name string
    status.workspace_name = read_jj_workspace_name(repo_path);
    status.is_default_workspace = status.workspace_name == "default";

    Ok(status)
}

/// Read the jj workspace name from the checkout file.
///
/// The `.jj/working_copy/checkout` file ends with the workspace name
/// as trailing ASCII bytes (length-prefixed in a binary format).
fn read_jj_workspace_name(repo_path: &Path) -> String {
    let checkout_path = repo_path.join(".jj/working_copy/checkout");
    let Ok(data) = std::fs::read(&checkout_path) else {
        return "default".to_string();
    };

    // Read backwards from the end to find the ASCII workspace name
    let end = data.len();
    let mut start = end;
    while start > 0
        && (data[start - 1].is_ascii_alphanumeric()
            || data[start - 1] == b'-'
            || data[start - 1] == b'_')
    {
        start -= 1;
    }
    if start < end {
        String::from_utf8_lossy(&data[start..end]).to_string()
    } else {
        "default".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_jj_repo() -> TempDir {
        let dir = TempDir::new().unwrap();
        let output = Command::new("jj")
            .args(["git", "init"])
            .current_dir(dir.path())
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "jj git init failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        dir
    }

    async fn jj_cmd(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("jj")
            .args(args)
            .current_dir(repo)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "jj {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).to_string()
    }

    #[tokio::test]
    async fn test_empty_repo() {
        let dir = create_jj_repo().await;
        let config = Config {
            color: false,
            ..Default::default()
        };
        let status = query_jj_status(dir.path(), &config, false).await.unwrap();
        assert!(!status.change_id.is_empty());
        assert!(status.empty);
        assert!(status.bookmarks.is_empty());
    }

    #[tokio::test]
    async fn test_with_description() {
        let dir = create_jj_repo().await;
        jj_cmd(dir.path(), &["describe", "-m", "hello world"]).await;
        let config = Config {
            color: false,
            ..Default::default()
        };
        let status = query_jj_status(dir.path(), &config, false).await.unwrap();
        assert_eq!(status.description, "hello world");
    }

    #[tokio::test]
    async fn test_with_bookmark() {
        let dir = create_jj_repo().await;
        jj_cmd(dir.path(), &["bookmark", "create", "main", "-r", "@"]).await;
        let config = Config {
            color: false,
            ..Default::default()
        };
        let status = query_jj_status(dir.path(), &config, false).await.unwrap();
        assert!(
            status
                .bookmarks
                .iter()
                .any(|b| b.name == "main" && b.distance == 0 && b.display == "main")
        );
    }

    #[tokio::test]
    async fn test_bookmark_distance() {
        let dir = create_jj_repo().await;
        jj_cmd(dir.path(), &["bookmark", "create", "main", "-r", "@"]).await;
        jj_cmd(dir.path(), &["new"]).await;
        let config = Config {
            color: false,
            ..Default::default()
        };
        let status = query_jj_status(dir.path(), &config, false).await.unwrap();
        assert!(
            status
                .bookmarks
                .iter()
                .any(|b| b.name == "main" && b.distance == 1 && b.display == "main+1")
        );
    }

    #[tokio::test]
    async fn test_default_workspace() {
        let dir = create_jj_repo().await;
        let config = Config {
            color: false,
            ..Default::default()
        };
        let status = query_jj_status(dir.path(), &config, false).await.unwrap();
        assert_eq!(status.workspace_name, "default");
        assert!(status.is_default_workspace);
    }

    #[tokio::test]
    async fn test_named_workspace() {
        let dir = create_jj_repo().await;
        let work2_dir = TempDir::with_prefix("jj-ws-").unwrap();
        // jj workspace add needs a non-existing or empty dir — use a subdir of the temp
        let work2 = work2_dir.path().join("secondary");
        jj_cmd(
            dir.path(),
            &[
                "workspace",
                "add",
                "--name",
                "secondary",
                work2.to_str().unwrap(),
            ],
        )
        .await;

        let config = Config {
            color: false,
            ..Default::default()
        };

        // Query from the secondary workspace
        let status = query_jj_status(&work2, &config, false).await.unwrap();
        assert_eq!(status.workspace_name, "secondary");
        assert!(!status.is_default_workspace);

        // Original workspace is still "default"
        let status = query_jj_status(dir.path(), &config, false).await.unwrap();
        assert_eq!(status.workspace_name, "default");
        assert!(status.is_default_workspace);
    }

    #[tokio::test]
    async fn test_diff_stats() {
        let dir = create_jj_repo().await;
        std::fs::write(dir.path().join("test.txt"), "hello\nworld\n").unwrap();
        let config = Config {
            color: false,
            ..Default::default()
        };
        let status = query_jj_status(dir.path(), &config, false).await.unwrap();
        assert!(status.files_changed >= 1);
        assert!(status.lines_added > 0);
        // For jj, total should equal unstaged (no staging area)
        assert_eq!(status.total_files_changed, status.files_changed);
        assert_eq!(status.total_lines_added, status.lines_added);
        assert_eq!(status.total_lines_removed, status.lines_removed);
        assert_eq!(status.staged_files_changed, 0);
    }
}
