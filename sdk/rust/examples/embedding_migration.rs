use ketebe_sdk::{Client, ClientConfig, StartEmbeddingMigration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(ClientConfig::new("http://127.0.0.1:8080"))?;

    client
        .start_embedding_migration(
            "docs",
            &StartEmbeddingMigration {
                target_profile: "text-embedding-v2".into(),
            },
        )
        .await?;

    let migration = client.get_embedding_migration("docs").await?;
    println!("migration status: {:?}", migration.fields);

    let job = client
        .start_embedding_migration_catch_up_job("docs")
        .await?;
    println!("catch-up job: {} ({})", job.id, job.state);
    Ok(())
}
