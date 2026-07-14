use std::num::NonZeroUsize;

use futures_util::StreamExt;
use tapo::responses::EnergyUsageResult;

type DataReceiver = z_queue::defaults::BoundedReceiver<Result<EnergyUsageResult, tapo::Error>>;
type DataSender = z_queue::defaults::BoundedSender<Result<EnergyUsageResult, tapo::Error>>;

type CommandReceiver = z_queue::defaults::BoundedReceiver<()>;
type CommandSender = z_queue::defaults::BoundedSender<()>;

pub struct TapoClient {
    command_tx: CommandSender,
    data_rx: DataReceiver,
    _handle: std::thread::JoinHandle<()>,
}

impl TapoClient {
    pub fn new(ip: String, email: String, password: String) -> Self {
        let (command_tx, command_rx) = z_queue::bounded(NonZeroUsize::new(20).unwrap());
        let (data_tx, data_rx) = z_queue::bounded(NonZeroUsize::new(20).unwrap());

        let handle = std::thread::spawn(move || thread(ip, email, password, command_rx, data_tx));
        Self { command_tx, data_rx, _handle: handle }
    }

    pub async fn fetch_p110_energy(&self) -> Result<EnergyUsageResult, tapo::Error> {
        let data_future = self.data_rx.recv_async();
        _ = self.command_tx.send_async(()).await;
        data_future.await.expect("Failed to receive data")
    }
}

fn thread(
    ip: String,
    email: String,
    password: String,
    command_rx: CommandReceiver,
    data_tx: DataSender,
) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(async move {
            let mut device_opt: Option<tapo::PlugEnergyMonitoringHandler> = None;
            let mut is_retry = false;

            let mut commands = command_rx.into_stream();
            while let Some(()) = commands.next().await {
                if device_opt.is_none() {
                    match tapo::ApiClient::new(&email, &password).p110(&ip).await {
                        Ok(device) => device_opt = Some(device),
                        Err(error) => {
                            println!("Failed to connect to device: {error}");
                            if data_tx.send_async(Err(error)).await.is_err() {
                                break;
                            }
                        }
                    }
                }

                let device = device_opt.as_ref().unwrap();

                let result = device.get_energy_usage().await;

                if result.is_err() && !is_retry {
                    device_opt = None;
                    is_retry = true;
                    continue;
                }

                is_retry = false;

                if data_tx.send_async(result).await.is_err() {
                    break;
                }
            }
        });
}
