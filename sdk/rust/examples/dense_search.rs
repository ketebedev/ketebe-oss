use ketebe_sdk::{Client, ClientConfig, QueryRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(ClientConfig::new("http://127.0.0.1:8080"))?;
    let result = client
        .query(
            "docs",
            &QueryRequest {
                vector: Some(vec![1.0, 0.0, 0.0]),
                top_k: Some(10),
                explain: true,
                ..QueryRequest::default()
            },
        )
        .await?;

    for hit in result.hits {
        println!("{:?} score={}", hit.id, hit.score);
    }
    Ok(())
}
