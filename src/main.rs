use clap::Parser;
use color_eyre::eyre::Result;

/// Inara — terminal-native schema explorer and migration generator for sqlx + Postgres.
#[derive(Parser, Debug)]
#[command(name = "inara", version, about)]
struct Cli {
    /// PostgreSQL connection URL (e.g. postgres://user:pass@localhost/dbname).
    /// Falls back to the DATABASE_URL environment variable.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match &cli.database_url {
        Some(url) => {
            // Mask password in output for safety
            let display_url = mask_password(url);
            tracing::info!("Database URL: {display_url}");
            println!("inara: ready to connect to {display_url}");
        }
        None => {
            eprintln!(
                "No database URL provided.\n\n\
                 Use --database-url <URL> or set the DATABASE_URL environment variable.\n\n\
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
