use futures::StreamExt;
use futures_concurrency::future::Join;
use serde_json::json;
use time::OffsetDateTime;
use tokio_stream::wrappers::LinesStream;

use std::error::Error;
use std::future::ready;

use tokio::io::{self, AsyncBufReadExt};

use clap::Parser;
use sifis_dht::domobroker::{create_domo_broker, DomoBrokerConf, DomoBrokerSender};
use sifis_dht::domocache::DomoEvent;

#[derive(Parser, Debug)]
struct Opt {
    /// Path to a sqlite file
    #[clap(parse(try_from_str))]
    sqlite_file: String,

    /// Path to a private key file
    #[clap(parse(try_from_str))]
    private_key_file: String,

    /// Use a persistent cache
    #[clap(parse(try_from_str))]
    is_persistent_cache: bool,

    /// 32 bytes long shared key in hex format
    #[clap(parse(try_from_str))]
    shared_key: String,

    /// HTTP port
    #[clap(parse(try_from_str))]
    http_port: u16,

    /// use only loopback iface for libp2p
    #[clap(parse(try_from_str))]
    loopback_only: bool,

    /// size of message buffer
    #[clap(long)]
    message_buffer_size: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let opt = Opt::parse();

    let local = OffsetDateTime::now_utc();

    log::info!("Program started at {:?}", local);

    let Opt {
        sqlite_file,
        private_key_file,
        is_persistent_cache,
        shared_key,
        http_port,
        loopback_only,
        message_buffer_size,
    } = opt;

    env_logger::init();

    let stdin = io::BufReader::new(io::stdin()).lines();
    let debug_console = std::env::var("DHT_DEBUG_CONSOLE").is_ok();

    let domo_broker_conf = DomoBrokerConf {
        sqlite_file,
        private_key_file: Some(private_key_file),
        is_persistent_cache,
        shared_key,
        http_port,
        loopback_only,
        message_buffer_size,
    };

    let (domo_broker_sender, domo_broker_receiver) = create_domo_broker(domo_broker_conf).await?;

    let domo_broker_future = domo_broker_receiver.into_stream().for_each(|m| {
        report_event(&m);
        ready(())
    });

    if debug_console {
        let stdin_future = LinesStream::new(stdin).for_each(|line| async {
            if let Ok(line) = line {
                handle_user_input(&line, &domo_broker_sender).await;
            }
        });
        (domo_broker_future, stdin_future).join().await;
    } else {
        domo_broker_future.await;
    }
    Ok(())
}

fn report_event(m: &DomoEvent) {
    println!("Domo Event received");
    match m {
        DomoEvent::None => {}
        DomoEvent::VolatileData(_v) => {
            println!("Volatile");
        }
        DomoEvent::PersistentData(_v) => {
            println!("Persistent");
        }
    }
}

async fn handle_user_input(line: &str, domo_broker_sender: &DomoBrokerSender) {
    let mut args = line.split(' ');

    match args.next() {
        Some("HASH") => {
            domo_broker_sender.print_cache_hash().await;
        }
        Some("PRINT") => domo_broker_sender.print().await,
        Some("PEERS") => {
            println!("Own peer ID: {}", domo_broker_sender.local_peer_id());
            println!("Peers:");
            domo_broker_sender.print_peers_cache().await
        }
        Some("DEL") => {
            if let (Some(topic_name), Some(topic_uuid)) = (args.next(), args.next()) {
                domo_broker_sender
                    .delete_value(topic_name, topic_uuid)
                    .await;
            } else {
                println!("topic_name, topic_uuid are mandatory arguments");
            }
        }
        Some("PUB") => {
            let value = args.next();

            if value.is_none() {
                println!("value is mandatory");
            }

            let val = json!({ "payload": value });

            domo_broker_sender.pub_value(val).await;
        }
        Some("PUT") => {
            let arguments = (args.next(), args.next(), args.next());

            if let (Some(topic_name), Some(topic_uuid), Some(value)) = arguments {
                println!("{topic_name} {topic_uuid} {value}");

                let val = json!({ "payload": value });

                domo_broker_sender
                    .write_value(topic_name, topic_uuid, val)
                    .await;
            } else {
                println!("topic_name, topic_uuid, values are mandatory arguments");
            }
        }
        _ => {
            println!("Commands:");
            println!("HASH");
            println!("PRINT");
            println!("PEERS");
            println!("PUB <value>");
            println!("PUT <topic_name> <topic_uuid> <value>");
            println!("DEL <topic_name> <topic_uuid>");
        }
    }
}
