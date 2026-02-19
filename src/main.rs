use clap::Parser;
use color_eyre::eyre::Result;

/// Inara — terminal-native schema explorer and migration generator for sqlx + Postgres.
#[derive(Parser, Debug)]
#[command(name = "inara", version, about)]
struct Cli {
    /// PostgreSQL connection URL (e.g. postgres://user:pass@localhost/dbname).
    /// Falls back to the DATABASE_URL environment variable, then inara.toml.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file if present (before Cli::parse so DATABASE_URL is available to clap)
    let _ = dotenvy::dotenv();

    let cli = Cli::parse();

    // Discover and load inara.toml config
    let loaded = inara::config::find_and_load();

    // Resolve database URL: CLI/env > config file
    let database_url = cli.database_url.or_else(|| {
        loaded
            .as_ref()
            .and_then(|l| inara::config::resolve_database_url(&l.config))
            .map(String::from)
    });

    // Resolve migrations directory
    let migrations_dir = inara::config::resolve_migrations_dir(
        loaded.as_ref().map(|l| &l.config),
        loaded.as_ref().map(|l| l.config_dir.as_path()),
    );

    // Collect type overrides from config
    let config_overrides = loaded
        .as_ref()
        .map(|l| l.config.types.overrides.clone())
        .unwrap_or_default();

    match database_url {
        Some(ref url) => {
            let display_url = mask_password(url);
            inara::tui::run(url, display_url, migrations_dir, config_overrides).await?;
        }
        None => {
            eprintln!(
                "No database URL provided.\n\n\
                 Use --database-url <URL>, set the DATABASE_URL environment variable,\n\
                 or add database_url to inara.toml.\n\n\
                 Example:\n  \
                 inara --database-url postgres://user:pass@localhost/mydb\n  \
                 DATABASE_URL=postgres://... inara"
            );
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Mask the password portion of a Postgres connection URL for display.
fn mask_password(url: &str) -> String {
    // Pattern: postgres://user:password@host...
    // Replace password between first ':' after '//' and '@'
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];
        if let Some(at_pos) = after_scheme.find('@') {
            let userinfo = &after_scheme[..at_pos];
            if let Some(colon_pos) = userinfo.find(':') {
                let user = &userinfo[..colon_pos];
                let rest = &after_scheme[at_pos..];
                return format!("{}://{}:***{}", &url[..scheme_end], user, rest);
            }
        }
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_password_with_password() {
        let url = "postgres://user:secret@localhost/db";
        assert_eq!(mask_password(url), "postgres://user:***@localhost/db");
    }

    #[test]
    fn mask_password_without_password() {
        let url = "postgres://user@localhost/db";
        assert_eq!(mask_password(url), "postgres://user@localhost/db");
    }

    #[test]
    fn mask_password_no_userinfo() {
        let url = "postgres://localhost/db";
        assert_eq!(mask_password(url), "postgres://localhost/db");
    }

    #[test]
    fn mask_password_complex_url() {
        let url = "postgres://admin:p%40ss@host:5432/mydb?sslmode=require";
        assert_eq!(
            mask_password(url),
            "postgres://admin:***@host:5432/mydb?sslmode=require"
        );
    }
}
