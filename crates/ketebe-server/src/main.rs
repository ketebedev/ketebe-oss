#![forbid(unsafe_code)]

#[tokio::main]
async fn main() {
    ketebe_server::run_standalone_from_env().await;
}
