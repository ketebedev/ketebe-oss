use ketebe_sdk::{Client, ClientConfig, DocumentUpsert, RecordId};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(ClientConfig::new("http://127.0.0.1:8080"))?;
    client
        .upsert_document(
            "docs",
            &RecordId::String("architecture-rfc".into()),
            &DocumentUpsert {
                text: "Ketebe is an AI-native retrieval platform.".into(),
                metadata: Some(json!({"category": "rfc", "language": "en"})),
                source: Some(json!({"uri": "https://example.invalid/rfc/ketebe"})),
                chunking: None,
            },
        )
        .await?;

    println!("document accepted for ingestion");
    Ok(())
}
