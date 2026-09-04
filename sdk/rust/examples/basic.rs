use ketebe_sdk::{Client, ClientConfig, CreateCollection, QueryRequest, RecordId, RecordUpsert};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(ClientConfig::new("http://127.0.0.1:8080"))?;

    client
        .create_collection(&CreateCollection {
            id: "docs".into(),
            dimension: 3,
            metric: "cosine".into(),
            lexical_fields: None,
        })
        .await?;

    client
        .upsert_record(
            "docs",
            &RecordId::String("intro".into()),
            &RecordUpsert {
                vector: vec![1.0, 0.0, 0.0],
                metadata: None,
            },
        )
        .await?;

    let result = client
        .query(
            "docs",
            &QueryRequest {
                vector: Some(vec![1.0, 0.0, 0.0]),
                top_k: Some(5),
                explain: true,
                ..QueryRequest::default()
            },
        )
        .await?;

    println!("{} hits", result.hits.len());
    Ok(())
}
