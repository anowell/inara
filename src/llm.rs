// LLM integration for AI-assisted migration editing.
//
// Provides `:ai <prompt>` for editing the current migration via natural language,
// and `:generate-down` for generating a down migration from the up migration.
// Uses OPENAI_API_KEY (or compatible API) for communication.

use std::fmt::Write;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::schema::render::render_schema;
use crate::schema::Schema;

/// Shared handle for receiving async LLM results.
pub type LlmResultHandle = Arc<Mutex<Option<LlmResult>>>;

/// Create a new LLM result handle.
pub fn new_llm_handle() -> LlmResultHandle {
    Arc::new(Mutex::new(None))
}

/// Result from an LLM request.
#[derive(Debug, Clone)]
pub enum LlmResult {
    /// Successfully received a response.
    Success(String),
    /// An error occurred.
    Error(String),
}

/// LLM client configuration, read from environment.
#[derive(Debug, Clone)]
pub struct LlmConfig {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
}

impl LlmConfig {
    /// Try to load LLM configuration from environment variables.
    ///
    /// Returns `None` if `OPENAI_API_KEY` is not set.
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("OPENAI_API_KEY").ok()?;
        if api_key.is_empty() {
            return None;
        }
        let base_url = std::env::var("OPENAI_API_BASE")
            .or_else(|_| std::env::var("OPENAI_BASE_URL"))
            .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
        let model = std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o".to_string());
        Some(Self {
            api_key,
            base_url,
            model,
        })
    }

    /// Check if LLM is configured.
    pub fn is_configured() -> bool {
        std::env::var("OPENAI_API_KEY")
            .ok()
            .map(|k| !k.is_empty())
            .unwrap_or(false)
    }
}

/// Request body for the OpenAI chat completions API.
#[derive(Debug, Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: f32,
}

/// A single message in the chat completions API.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

/// Response body from the OpenAI chat completions API.
#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

/// A single choice in the chat completions response.
#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessageResponse,
}

/// Message within a chat completion choice.
#[derive(Debug, Deserialize)]
struct ChatMessageResponse {
    content: Option<String>,
}

/// Build the system prompt for `:ai` (migration editing).
pub fn build_ai_system_prompt(schema: &Schema, current_sql: &str) -> String {
    let schema_text = render_schema(schema);
    let mut prompt = String::new();
    let _ = writeln!(
        prompt,
        "You are a PostgreSQL migration assistant. You help edit SQL migrations."
    );
    let _ = writeln!(prompt);
    let _ = writeln!(prompt, "Current database schema:");
    let _ = writeln!(prompt, "```");
    let _ = write!(prompt, "{schema_text}");
    let _ = writeln!(prompt, "```");
    let _ = writeln!(prompt);
    let _ = writeln!(prompt, "Current migration SQL:");
    let _ = writeln!(prompt, "```sql");
    let _ = write!(prompt, "{current_sql}");
    let _ = writeln!(prompt, "```");
    let _ = writeln!(prompt);
    let _ = writeln!(
        prompt,
        "Respond with ONLY the updated SQL migration. No explanations, no markdown fences."
    );
    prompt
}

/// Build the system prompt for `:generate-down`.
pub fn build_generate_down_prompt(
    original_schema: &Schema,
    current_schema: &Schema,
    up_sql: &str,
) -> String {
    let original_text = render_schema(original_schema);
    let current_text = render_schema(current_schema);
    let mut prompt = String::new();
    let _ = writeln!(
        prompt,
        "You are a PostgreSQL migration assistant. Generate a DOWN migration that reverses the UP migration."
    );
    let _ = writeln!(prompt);
    let _ = writeln!(prompt, "Original schema (before migration):");
    let _ = writeln!(prompt, "```");
    let _ = write!(prompt, "{original_text}");
    let _ = writeln!(prompt, "```");
    let _ = writeln!(prompt);
    let _ = writeln!(prompt, "Current schema (after migration):");
    let _ = writeln!(prompt, "```");
    let _ = write!(prompt, "{current_text}");
    let _ = writeln!(prompt, "```");
    let _ = writeln!(prompt);
    let _ = writeln!(prompt, "UP migration:");
    let _ = writeln!(prompt, "```sql");
    let _ = write!(prompt, "{up_sql}");
    let _ = writeln!(prompt, "```");
    let _ = writeln!(prompt);
    let _ = writeln!(
        prompt,
        "Generate the corresponding DOWN migration SQL that reverses all changes made by the UP migration."
    );
    let _ = writeln!(
        prompt,
        "Respond with ONLY the SQL. No explanations, no markdown fences."
    );
    prompt
}

/// Extract SQL content from an LLM response.
///
/// Strips markdown code fences if the model includes them despite instructions.
pub fn extract_sql(response: &str) -> String {
    let trimmed = response.trim();

    // Strip ```sql ... ``` fences
    if let Some(rest) = trimmed.strip_prefix("```sql") {
        if let Some(inner) = rest.strip_suffix("```") {
            return inner.trim().to_string();
        }
    }

    // Strip ``` ... ``` fences (no language tag)
    if let Some(rest) = trimmed.strip_prefix("```") {
        if let Some(inner) = rest.strip_suffix("```") {
            return inner.trim().to_string();
        }
    }

    trimmed.to_string()
}

/// Trait for sending LLM requests, allowing mock implementations in tests.
pub trait LlmClient: Send + Sync {
    fn send(
        &self,
        config: &LlmConfig,
        messages: Vec<ChatMessage>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send>>;
}

/// Real HTTP-based LLM client using reqwest.
pub struct HttpLlmClient;

impl LlmClient for HttpLlmClient {
    fn send(
        &self,
        config: &LlmConfig,
        messages: Vec<ChatMessage>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send>> {
        let config = config.clone();
        Box::pin(async move {
            let client = reqwest::Client::new();
            let url = format!("{}/chat/completions", config.base_url);
            let request = ChatRequest {
                model: config.model,
                messages,
                temperature: 0.2,
            };

            let response = client
                .post(&url)
                .header("Authorization", format!("Bearer {}", config.api_key))
                .header("Content-Type", "application/json")
                .json(&request)
                .send()
                .await
                .map_err(|e| format!("HTTP request failed: {e}"))?;

            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(format!("API error ({status}): {body}"));
            }

            let chat_response: ChatResponse = response
                .json()
                .await
                .map_err(|e| format!("Failed to parse response: {e}"))?;

            chat_response
                .choices
                .first()
                .and_then(|c| c.message.content.clone())
                .ok_or_else(|| "Empty response from LLM".to_string())
        })
    }
}

/// Spawn an async LLM request for `:ai <prompt>`.
pub fn spawn_ai_request(
    schema: &Schema,
    migration_sql: &str,
    user_prompt: &str,
    handle: LlmResultHandle,
) {
    let config = match LlmConfig::from_env() {
        Some(c) => c,
        None => {
            if let Ok(mut guard) = handle.lock() {
                *guard = Some(LlmResult::Error("LLM not configured".to_string()));
            }
            return;
        }
    };

    let system = build_ai_system_prompt(schema, migration_sql);
    let messages = vec![
        ChatMessage {
            role: "system".to_string(),
            content: system,
        },
        ChatMessage {
            role: "user".to_string(),
            content: user_prompt.to_string(),
        },
    ];

    spawn_llm_request(config, messages, handle);
}

/// Spawn an async LLM request for `:generate-down`.
pub fn spawn_generate_down_request(
    original_schema: &Schema,
    current_schema: &Schema,
    up_sql: &str,
    handle: LlmResultHandle,
) {
    let config = match LlmConfig::from_env() {
        Some(c) => c,
        None => {
            if let Ok(mut guard) = handle.lock() {
                *guard = Some(LlmResult::Error("LLM not configured".to_string()));
            }
            return;
        }
    };

    let system = build_generate_down_prompt(original_schema, current_schema, up_sql);
    let messages = vec![
        ChatMessage {
            role: "system".to_string(),
            content: system,
        },
        ChatMessage {
            role: "user".to_string(),
            content: "Generate the down migration.".to_string(),
        },
    ];

    spawn_llm_request(config, messages, handle);
}

/// Spawn the actual async LLM HTTP request.
fn spawn_llm_request(config: LlmConfig, messages: Vec<ChatMessage>, handle: LlmResultHandle) {
    let handle_clone = handle.clone();
    let result = tokio::runtime::Handle::try_current().map(|rt| {
        rt.spawn(async move {
            let client = HttpLlmClient;
            let result = client.send(&config, messages).await;
            if let Ok(mut guard) = handle_clone.lock() {
                *guard = Some(match result {
                    Ok(content) => LlmResult::Success(extract_sql(&content)),
                    Err(e) => LlmResult::Error(e),
                });
            }
        });
    });

    // No runtime available — set error immediately
    if result.is_err() {
        if let Ok(mut guard) = handle.lock() {
            *guard = Some(LlmResult::Error("No async runtime available".to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::PgType;
    use crate::schema::{Column, Constraint, Table};

    fn sample_schema() -> Schema {
        let mut schema = Schema::new();
        let mut users = Table::new("users");
        users.add_column(Column::new("id", PgType::Uuid));
        users.add_column(Column::new("email", PgType::Text));
        users.add_constraint(Constraint::PrimaryKey {
            name: Some("users_pkey".into()),
            columns: vec!["id".into()],
        });
        schema.add_table(users);
        schema
    }

    // --- Config tests ---

    #[test]
    fn config_not_configured_when_env_unset() {
        // Clear env to test
        std::env::remove_var("OPENAI_API_KEY");
        assert!(!LlmConfig::is_configured());
        assert!(LlmConfig::from_env().is_none());
    }

    // --- Prompt construction ---

    #[test]
    fn ai_system_prompt_includes_schema_and_sql() {
        let schema = sample_schema();
        let sql = "ALTER TABLE users ADD COLUMN bio text;";
        let prompt = build_ai_system_prompt(&schema, sql);

        assert!(prompt.contains("users"), "should include table name");
        assert!(
            prompt.contains("ALTER TABLE"),
            "should include migration SQL"
        );
        assert!(
            prompt.contains("PostgreSQL migration assistant"),
            "should include role"
        );
        assert!(
            prompt.contains("ONLY the updated SQL"),
            "should instruct SQL-only output"
        );
    }

    #[test]
    fn ai_system_prompt_includes_columns() {
        let schema = sample_schema();
        let sql = "";
        let prompt = build_ai_system_prompt(&schema, sql);

        assert!(prompt.contains("email"), "should include column names");
        assert!(prompt.contains("uuid"), "should include column types");
    }

    #[test]
    fn generate_down_prompt_includes_both_schemas() {
        let original = sample_schema();
        let mut current = sample_schema();
        let users = current.tables.get_mut("users").unwrap();
        users.add_column(Column::new("bio", PgType::Text).nullable());

        let up_sql = "ALTER TABLE users ADD COLUMN bio text;";
        let prompt = build_generate_down_prompt(&original, &current, up_sql);

        assert!(
            prompt.contains("Original schema"),
            "should reference original"
        );
        assert!(
            prompt.contains("Current schema"),
            "should reference current"
        );
        assert!(prompt.contains("UP migration"), "should reference UP");
        assert!(prompt.contains("DOWN migration"), "should ask for DOWN");
        assert!(
            prompt.contains("bio"),
            "current schema should include new column"
        );
    }

    #[test]
    fn generate_down_prompt_includes_up_sql() {
        let schema = sample_schema();
        let up_sql =
            "ALTER TABLE users ADD COLUMN bio text;\nALTER TABLE users ADD COLUMN avatar text;";
        let prompt = build_generate_down_prompt(&schema, &schema, up_sql);

        assert!(prompt.contains("ALTER TABLE users ADD COLUMN bio text;"));
        assert!(prompt.contains("ALTER TABLE users ADD COLUMN avatar text;"));
    }

    // --- Response parsing ---

    #[test]
    fn extract_sql_plain() {
        let response = "ALTER TABLE users ADD COLUMN bio text;";
        assert_eq!(
            extract_sql(response),
            "ALTER TABLE users ADD COLUMN bio text;"
        );
    }

    #[test]
    fn extract_sql_with_fences() {
        let response = "```sql\nALTER TABLE users ADD COLUMN bio text;\n```";
        assert_eq!(
            extract_sql(response),
            "ALTER TABLE users ADD COLUMN bio text;"
        );
    }

    #[test]
    fn extract_sql_with_plain_fences() {
        let response = "```\nALTER TABLE users DROP COLUMN legacy;\n```";
        assert_eq!(
            extract_sql(response),
            "ALTER TABLE users DROP COLUMN legacy;"
        );
    }

    #[test]
    fn extract_sql_with_whitespace() {
        let response = "  \n  ALTER TABLE users ADD COLUMN bio text;  \n  ";
        assert_eq!(
            extract_sql(response),
            "ALTER TABLE users ADD COLUMN bio text;"
        );
    }

    #[test]
    fn extract_sql_multiline() {
        let response = "```sql\nALTER TABLE users ADD COLUMN bio text;\nALTER TABLE users ADD COLUMN avatar text;\n```";
        let result = extract_sql(response);
        assert!(result.contains("bio text;"));
        assert!(result.contains("avatar text;"));
    }

    #[test]
    fn extract_sql_empty_response() {
        assert_eq!(extract_sql(""), "");
        assert_eq!(extract_sql("  "), "");
    }

    #[test]
    fn extract_sql_no_fences_multiline() {
        let response =
            "ALTER TABLE users ADD COLUMN bio text;\nALTER TABLE users ADD COLUMN avatar text;";
        let result = extract_sql(response);
        assert_eq!(result, response);
    }

    // --- Handle tests ---

    #[test]
    fn llm_handle_starts_empty() {
        let handle = new_llm_handle();
        let guard = handle.lock().unwrap();
        assert!(guard.is_none());
    }

    #[test]
    fn llm_handle_receives_result() {
        let handle = new_llm_handle();
        {
            let mut guard = handle.lock().unwrap();
            *guard = Some(LlmResult::Success("SELECT 1;".to_string()));
        }
        let guard = handle.lock().unwrap();
        assert!(matches!(guard.as_ref(), Some(LlmResult::Success(_))));
        if let Some(LlmResult::Success(sql)) = guard.as_ref() {
            assert_eq!(sql, "SELECT 1;");
        }
    }

    #[test]
    fn llm_handle_receives_error() {
        let handle = new_llm_handle();
        {
            let mut guard = handle.lock().unwrap();
            *guard = Some(LlmResult::Error("API error".to_string()));
        }
        let guard = handle.lock().unwrap();
        assert!(matches!(guard.as_ref(), Some(LlmResult::Error(_))));
    }

    // --- Spawn without config ---

    #[test]
    fn spawn_ai_request_without_config_sets_error() {
        std::env::remove_var("OPENAI_API_KEY");
        let handle = new_llm_handle();
        let schema = sample_schema();
        spawn_ai_request(&schema, "SELECT 1;", "test", handle.clone());

        let guard = handle.lock().unwrap();
        match guard.as_ref() {
            Some(LlmResult::Error(msg)) => {
                assert!(msg.contains("not configured"), "got: {msg}");
            }
            other => panic!("expected Error, got: {other:?}"),
        }
    }

    #[test]
    fn spawn_generate_down_without_config_sets_error() {
        std::env::remove_var("OPENAI_API_KEY");
        let handle = new_llm_handle();
        let schema = sample_schema();
        spawn_generate_down_request(&schema, &schema, "SELECT 1;", handle.clone());

        let guard = handle.lock().unwrap();
        match guard.as_ref() {
            Some(LlmResult::Error(msg)) => {
                assert!(msg.contains("not configured"), "got: {msg}");
            }
            other => panic!("expected Error, got: {other:?}"),
        }
    }
}
