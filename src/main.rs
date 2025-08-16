use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use sui_rpc::{
    Client,
    field::{FieldMask, FieldMaskUtil},
    proto::sui::rpc::v2beta2::SubscribeCheckpointsRequest,
};
use sui_sdk::SuiClientBuilder;
use tokio_stream::StreamExt;

#[derive(Parser)]
pub struct Bench {
    #[arg(short, long)]
    pub rpc: String,
    #[arg(short, long)]
    pub grpc: bool,
}

#[tokio::main]
async fn main() {
    let args = Bench::parse();
    let mut stats = incr_stats::incr::Stats::new();
    if args.grpc {
        let mut client = Client::new(args.rpc).unwrap();
        let request = SubscribeCheckpointsRequest {
            read_mask: Some(FieldMask::from_str("transactions,summary")),
        };
        let mut stream = client
            .subscription_client()
            .subscribe_checkpoints(request)
            .await
            .unwrap()
            .into_inner();

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
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }
    }
}
