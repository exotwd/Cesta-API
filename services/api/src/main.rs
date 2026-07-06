#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cesta_api::run().await
}
