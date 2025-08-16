use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use sui_rpc::{
    field::{FieldMask, FieldMaskUtil}, proto::sui::rpc::v2beta2::{subscription_service_client::SubscriptionServiceClient, SubscribeCheckpointsRequest}, Client
};
use sui_sdk::SuiClientBuilder;
use tokio_stream::StreamExt;
// --- Add these imports for tonic ---
use tonic::{
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status,
};


#[derive(Parser)]
pub struct Bench {
    #[arg(short, long)]
    pub rpc: String,
    #[arg(short, long)]
    pub grpc: bool,
    #[arg(short, long)]
    pub token: Option<String>,
    #[arg(short, long, default_value_t=100)]
    pub poll: u64
}

#[tokio::main]
async fn main() {
    let args = Bench::parse();
    let mut stats = incr_stats::incr::Stats::new();
    if args.grpc {

        let endpoint = Endpoint::from_shared(args.rpc).unwrap()
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .unwrap();
        let channel = endpoint.connect().await.unwrap();

        let request = SubscribeCheckpointsRequest {
            read_mask: Some(FieldMask::from_str("transactions,summary")),
        };
        let mut stream = if let Some(token_str) = args.token {
            // Create a client with an interceptor that adds the token header
            SubscriptionServiceClient::with_interceptor(channel, move |mut req: Request<()>| {
                req.metadata_mut().insert("x-token", token_str.clone().try_into().unwrap());
                Ok(req)
            }).subscribe_checkpoints(request)
            .await
            .unwrap()
            .into_inner()
        } else {
            // Create a client without an interceptor
            SubscriptionServiceClient::new(channel).subscribe_checkpoints(request)
            .await
            .unwrap()
            .into_inner()
        };
        while let Some(it) = stream.next().await {
            let now = Utc::now();
            let it = it.unwrap();
            let ts = it.checkpoint.unwrap().summary.unwrap().timestamp.unwrap();
            let ckpt = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as _).unwrap();
            let elpsed = (now - ckpt).as_seconds_f64();
            stats.update(elpsed).unwrap();
            println!(
                "latency: mean = {:.03}, min = {:.03}, max = {:.03}, std = {:.03}",
                stats.mean().unwrap(),
                stats.min().unwrap(),
                stats.max().unwrap(),
                stats.sample_standard_deviation().unwrap_or_default()
            );
        }
    } else {
        let client = SuiClientBuilder::default().build(&args.rpc).await.unwrap();
        let mut ckpt = client
            .read_api()
            .get_latest_checkpoint_sequence_number()
            .await
            .unwrap()
            + 1;
        loop {
            match client.read_api().get_checkpoint(ckpt.into()).await {
                Ok(c) => {
                    let now = Utc::now();
                    let ts = chrono::DateTime::from_timestamp_millis(c.timestamp_ms as _).unwrap();
                    let elpsed = (now - ts).as_seconds_f64();
                    stats.update(elpsed).unwrap();
                    println!(
                        "latency: mean = {:.03}, min = {:.03}, max = {:.03}, std = {:.03}",
                        stats.mean().unwrap(),
                        stats.min().unwrap(),
                        stats.max().unwrap(),
                        stats.sample_standard_deviation().unwrap_or_default()
                    );
                    ckpt += 1;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(args.poll)).await;
                    continue;
                }
            }
        }
    }
}
