use std::path::PathBuf;

use anyhow::{anyhow, Result};
use clap::Parser;
use console::style;
use duct::cmd;

#[derive(clap::Subcommand, Debug)]
enum CopyTestAction {
    Day1,
    Day2,
    Day3,
    Day4,
}

#[derive(clap::Subcommand, Debug)]
enum Action {
    /// Check.
    Check,
    /// Build and serve book.
    Book,
    /// Install necessary tools for development.
    InstallTools,
    /// Show environment variables.
    Show,
    /// Run CI jobs
    Ci,
    /// Sync starter repo and reference solution.
    Sync,
    /// Check starter code
    Scheck,
    /// Copy test cases
    #[command(subcommand)]
    CopyTest(CopyTestAction),
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    action: Action,
}

fn switch_to_workspace_root() -> Result<()> {
    std::env::set_current_dir(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .ok_or_else(|| anyhow!("failed to find the workspace root"))?,
    )?;
    Ok(())
}

fn switch_to_starter_root() -> Result<()> {
    std::env::set_current_dir(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .ok_or_else(|| anyhow!("failed to find the workspace root"))?
            .join("mini-lsm-starter"),
    )?;
    Ok(())
}

fn fmt() -> Result<()> {
    println!("{}", style("cargo fmt").bold());
    cmd!("cargo", "fmt").run()?;
    Ok(())
}

fn check_fmt() -> Result<()> {
    println!("{}", style("cargo fmt --check").bold());
    cmd!("cargo", "fmt", "--check").run()?;
    Ok(())
}

fn check() -> Result<()> {
    println!("{}", style("cargo check").bold());
    cmd!("cargo", "check", "--all-targets").run()?;
    Ok(())
}

fn test() -> Result<()> {
    println!("{}", style("cargo nextest run --nocapture").bold());
    cmd!("cargo", "nextest", "run").run()?;
    Ok(())
}

fn clippy() -> Result<()> {
    println!("{}", style("cargo clippy").bold());
    cmd!("cargo", "clippy", "--all-targets").run()?;
    Ok(())
}

fn build_book() -> Result<()> {
    println!("{}", style("mdbook build").bold());
    cmd!("mdbook", "build").dir("mini-lsm-book").run()?;
    Ok(())
}

fn serve_book() -> Result<()> {
    println!("{}", style("mdbook serve").bold());
    cmd!("mdbook", "serve").dir("mini-lsm-book").run()?;
    Ok(())
}

fn sync() -> Result<()> {
    cmd!("mkdir", "-p", "sync-tmp").run()?;
    cmd!("cp", "-a", "mini-lsm-starter/", "sync-tmp/mini-lsm-starter").run()?;
    let cargo_toml = "sync-tmp/mini-lsm-starter/Cargo.toml";
    std::fs::write(
        cargo_toml,
        std::fs::read_to_string(cargo_toml)?.replace("mini-lsm-starter", "mini-lsm")
            + "\n[workspace]\n",
    )?;
    cmd!(
        "cargo",
        "semver-checks",
        "check-release",
        "--manifest-path",
        cargo_toml,
        "--baseline-root",
        "mini-lsm/Cargo.toml",
    )
    .run()?;
    Ok(())
}

fn copy_test_case(test: CopyTestAction) -> Result<()> {
    match test {
        CopyTestAction::Day1 => {
            cmd!(
                "cp",
                "mini-lsm/src/block/tests.rs",
                "mini-lsm-starter/src/block/tests.rs"
            )
            .run()?;
        }
        CopyTestAction::Day2 => {
            cmd!(
                "cp",
                "mini-lsm/src/table/tests.rs",
                "mini-lsm-starter/src/table/tests.rs"
            )
            .run()?;
        }
        CopyTestAction::Day3 => {
            cmd!(
                "cp",
                "mini-lsm/src/mem_table/tests.rs",
                "mini-lsm-starter/src/mem_table/tests.rs"
            )
            .run()?;
            cmd!(
                "cp",
                "mini-lsm/src/iterators/tests/merge_iterator_test.rs",
                "mini-lsm-starter/src/iterators/tests/merge_iterator_test.rs"
            )
            .run()?;
            cmd!(
                "cp",
                "mini-lsm/src/iterators/tests/two_merge_iterator_test.rs",
                "mini-lsm-starter/src/iterators/tests/two_merge_iterator_test.rs"
            )
            .run()?;
            cmd!(
                "cp",
                "mini-lsm/src/iterators/tests.rs",
                "mini-lsm-starter/src/iterators/tests.rs"
            )
            .run()?;
        }
        CopyTestAction::Day4 => {
            cmd!(
                "cp",
                "mini-lsm/src/tests/day4_tests.rs",
                "mini-lsm-starter/src/tests/day4_tests.rs"
            )
            .run()?;
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    match args.action {
        Action::InstallTools => {
            println!("{}", style("cargo install cargo-nextest").bold());
            cmd!("cargo", "install", "cargo-nextest", "--locked").run()?;
            println!("{}", style("cargo install mdbook mdbook-toc").bold());
            cmd!("cargo", "install", "mdbook", "mdbook-toc", "--locked").run()?;
            println!("{}", style("cargo install cargo-semver-checks").bold());
            cmd!("cargo", "install", "cargo-semver-checks", "--locked").run()?;
        }
        Action::Check => {
            switch_to_workspace_root()?;
            fmt()?;
            check()?;
            test()?;
            clippy()?;
        }
        Action::Scheck => {
            switch_to_starter_root()?;
            fmt()?;
            check()?;
            test()?;
            clippy()?;
        }
        Action::Book => {
            switch_to_workspace_root()?;
            serve_book()?;
        }
        Action::Ci => {
            switch_to_workspace_root()?;
            check_fmt()?;
            check()?;
            test()?;
            clippy()?;
            build_book()?;
        }
        Action::Show => {
            println!("CARGO_MANIFEST_DIR={}", env!("CARGO_MANIFEST_DIR"));
            println!("PWD={:?}", std::env::current_dir()?);
        }
        Action::Sync => {
            switch_to_workspace_root()?;
            sync()?;
        }
        Action::CopyTest(test) => {
            switch_to_workspace_root()?;
            copy_test_case(test)?;
        }
    }

    Ok(())
}
