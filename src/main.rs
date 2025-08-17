use std::{str::FromStr, time::Duration};

use chrono::Utc;
use clap::Parser;
use fastcrypto::hash::HashFunction;
use fastcrypto::traits::Signer;
use shared_crypto::intent::{Intent, IntentMessage};
use sui_rpc::{
    Client,
    field::{FieldMask, FieldMaskUtil},
    proto::sui::rpc::v2beta2::{
        ExecuteTransactionRequest, GetObjectRequest, GetTransactionRequest, SignatureScheme,
        SimpleSignature, SubscribeCheckpointsRequest, Transaction, UserSignature,
        ledger_service_client::LedgerServiceClient,
        subscription_service_client::SubscriptionServiceClient,
        transaction_execution_service_client::TransactionExecutionServiceClient,
        transaction_finality::Finality, user_signature::Signature,
    },
};
use sui_sdk::SuiClientBuilder;
use sui_types::{
    Identifier,
    balance_change::BalanceChange,
    base_types::{ObjectID, ObjectRef, SuiAddress},
    crypto::{SuiKeyPair, SuiSignature},
    digests::{ObjectDigest, TransactionDigest},
    effects::{TransactionEffects, TransactionEffectsAPI},
    event::Event,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{TransactionData, TransactionKind},
};
use tokio_stream::StreamExt;
use tonic::{
    Request, Status,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

#[derive(Clone, Debug)]
pub struct Auth(pub Option<String>);

impl tonic::service::Interceptor for Auth {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(s) = &self.0 {
            req.metadata_mut()
                .insert("x-token", s.clone().try_into().unwrap());
        }

        Ok(req)
    }
}

pub type CH = tonic::service::interceptor::InterceptedService<tonic::transport::Channel, Auth>;
pub async fn get_object_ref(grpc: &mut LedgerServiceClient<CH>, object_id: ObjectID) -> ObjectRef {
    let req = GetObjectRequest {
        object_id: Some(object_id.to_canonical_string(true)),
        version: None,
        read_mask: Some(FieldMask::from_str("object_id,version,digest")),
    };

    // https://github.com/hyperium/tonic/issues/285
    let object = grpc
        .get_object(req)
        .await
        .unwrap()
        .into_inner()
        .object
        .unwrap();
    (
        ObjectID::from_str(&object.object_id.unwrap()).unwrap(),
        object.version.unwrap().into(),
        ObjectDigest::from_str(&object.digest.unwrap()).unwrap(),
    )
}

pub async fn transaction_checkpoint(
    grpc: &mut LedgerServiceClient<CH>,
    digest: String,
) -> Option<u64> {
    let req = GetTransactionRequest {
        digest: Some(digest),
        read_mask: Some(FieldMask::from_str("transaction,checkpoint")),
    };
    match grpc.get_transaction(req).await {
        Ok(tx) => tx.into_inner().transaction.unwrap().checkpoint,
        Err(e) => match e.code() {
            tonic::Code::NotFound => None,
            _ => panic!("fail {}", e),
        },
    }
}

pub async fn sign_and_execute_transaction(
    grpc: &mut TransactionExecutionServiceClient<CH>,
    tx: TransactionData,
    kp: &SuiKeyPair,
) -> TransactionDigest {
    let tx_digest = tx.digest();
    let intent_msg = IntentMessage::new(Intent::sui_transaction(), &tx);
    let raw_tx = bcs::to_bytes(&intent_msg).unwrap();
    let mut hasher = sui_types::crypto::DefaultHash::default();
    hasher.update(raw_tx.clone());
    let digest = hasher.finalize().digest;
    let sui_sig = kp.sign(&digest);

    let scheme = match kp {
        SuiKeyPair::Ed25519(_) => SignatureScheme::Ed25519,
        SuiKeyPair::Secp256k1(_) => SignatureScheme::Secp256k1,
        SuiKeyPair::Secp256r1(_) => SignatureScheme::Secp256r1,
    };
    let req = ExecuteTransactionRequest {
        transaction: Some(Transaction::from(tx)),
        signatures: vec![UserSignature {
            bcs: None,
            scheme: Some(scheme.into()),
            signature: Some(Signature::Simple(SimpleSignature {
                scheme: Some(scheme.into()),
                // Use signature_bytes() here
                signature: Some(sui_sig.signature_bytes().to_vec().into()),
                public_key: Some(kp.public().as_ref().to_vec().into()),
            })),
        }],
        read_mask: Some(FieldMask::from_str(
            "finality,transaction,transaction.effects,transaction.events,transaction.balance_changes",
        )),
    };

    let resp = grpc.execute_transaction(req).await.unwrap().into_inner();
    let finality = resp.finality.unwrap().finality.unwrap();
    eprintln!("Finality: {:?}", &finality);
    let effects: TransactionEffects = bcs::from_bytes(
        resp.transaction
            .unwrap()
            .effects
            .unwrap()
            .bcs
            .unwrap()
            .value(),
    )
    .unwrap();
    let status = effects.status();

    if status.is_err() {
        panic!("Tx not on chain")
    } else {
        eprintln!("{} sent", tx_digest);
        tx_digest
    }
}

#[derive(Parser)]
pub struct Bench {
    #[arg(short, long)]
    pub rpc: String,
    #[arg(short, long)]
    pub grpc: bool,
    #[arg(short, long)]
    pub token: Option<String>,
    #[arg(short, long, default_value_t = 100)]
    pub poll: u64,
    #[arg(short, long)]
    pub gas_object: Option<ObjectID>,
    #[arg(long)]
    pub rgp: u64,
    #[arg(short, long)]
    pub send: bool,
}

#[tokio::main]
async fn main() {
    let args = Bench::parse();
    let mut stats = incr_stats::incr::Stats::new();
    let mut tx_stats = incr_stats::incr::Stats::new();

    let pass = if args.send {
        let password = rpassword::prompt_password("Priv key: ").unwrap();
        Some(SuiKeyPair::decode(&password).unwrap())
    } else {
        None
    };

    let auth = Auth(args.token.clone());

    if args.grpc {
        let endpoint = Endpoint::from_shared(args.rpc)
            .unwrap()
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .unwrap();
        let channel = endpoint.connect().await.unwrap();

        let request = SubscribeCheckpointsRequest {
            read_mask: Some(FieldMask::from_str("transactions,summary")),
        };

        let mut ledger = LedgerServiceClient::with_interceptor(channel.clone(), auth.clone());
        let mut subscribe =
            SubscriptionServiceClient::with_interceptor(channel.clone(), auth.clone());
        let mut exec =
            TransactionExecutionServiceClient::with_interceptor(channel.clone(), auth.clone());

        let mut stream = subscribe
            .subscribe_checkpoints(request)
            .await
            .unwrap()
            .into_inner();
        let mut pending: Option<(u64, TransactionDigest)> = None;
        while let Some(it) = stream.next().await {
            let now = Utc::now();
            let it = it.unwrap();
            let ck = it.checkpoint.unwrap();
            let sm = ck.summary.unwrap();
            let ts = sm.timestamp.unwrap();
            let ckpt = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as _).unwrap();
            let elpsed = (now - ckpt).as_seconds_f64();
            stats.update(elpsed).unwrap();
            println!(
                "block latency: mean = {:.03}, min = {:.03}, max = {:.03}, std = {:.03}",
                stats.mean().unwrap(),
                stats.min().unwrap(),
                stats.max().unwrap(),
                stats.sample_standard_deviation().unwrap_or_default()
            );

            if let Some((pckpt, pending)) = pending {
                for tx in ck.transactions {
                    let digest = tx.digest.unwrap();
                    if digest == pending.to_string() {
                        let lat = sm.sequence_number.unwrap() - pckpt;
                        tx_stats.update(lat as _).unwrap();
                        eprintln!("{} on chain after {} blocks", pending, pckpt);
                        println!(
                            "tx block latency: mean = {:.03}, min = {:.03}, max = {:.03}, std = {:.03}",
                            tx_stats.mean().unwrap(),
                            tx_stats.min().unwrap(),
                            tx_stats.max().unwrap(),
                            tx_stats.sample_standard_deviation().unwrap_or_default()
                        );
                        break;
                    }
                }
            }

            if pending.is_none() {
                if let Some(pass) = &pass {
                    let sender = SuiAddress::from(&pass.public());
                    let gas_coin = get_object_ref(&mut ledger, args.gas_object.unwrap()).await;
                    let mut builder = ProgrammableTransactionBuilder::new();
                    builder
                        .move_call(
                            ObjectID::from_str("0x2").unwrap(),
                            Identifier::new("address").unwrap(),
                            Identifier::new("length").unwrap(),
                            vec![],
                            vec![],
                        )
                        .unwrap();
                    let ptb = builder.finish();
                    let tx = TransactionData::new(
                        TransactionKind::ProgrammableTransaction(ptb),
                        sender,
                        gas_coin,
                        1_000_000_000,
                        args.rgp,
                    );
                    let digest = sign_and_execute_transaction(&mut exec, tx, pass).await;
                    pending = Some((sm.sequence_number.unwrap(), digest));
                };
            }
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
