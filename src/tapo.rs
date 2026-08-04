use std::num::NonZeroUsize;
use std::time::Duration;

use futures_util::StreamExt;
use tapo::responses::EnergyUsageResult;

/// How long the worker leaves the plug alone after a reading it could not get.
///
/// A plug that is off refuses the connection straight away, so without this the monitor's request
/// every quarter second turns into a tight reconnect loop against nothing.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);

type EnergyResult = Result<EnergyUsageResult, tapo::Error>;

type DataReceiver = z_queue::defaults::BoundedReceiver<EnergyResult>;
type DataSender = z_queue::defaults::BoundedSender<EnergyResult>;

type CommandReceiver = z_queue::defaults::BoundedReceiver<()>;
type CommandSender = z_queue::defaults::BoundedSender<()>;

pub struct TapoClient {
    command_tx: CommandSender,
    data_rx: DataReceiver,
    _handle: std::thread::JoinHandle<()>,
}

impl TapoClient {
    pub fn new(ip: String, email: String, password: String) -> Self {
        // One slot each way. The monitor only ever wants the newest reading, so a backlog would
        // do nothing but hand it older ones, and a full command queue already means a request is
        // outstanding.
        let (command_tx, command_rx) = z_queue::bounded(NonZeroUsize::MIN);
        let (data_tx, data_rx) = z_queue::bounded(NonZeroUsize::MIN);

        let handle = std::thread::spawn(move || thread(ip, email, password, command_rx, data_tx));
        Self { command_tx, data_rx, _handle: handle }
    }

    /// Whatever the plug has answered since this was last called, and a request for the next
    /// reading.
    ///
    /// Never waits on the plug. The monitor used to send a command and await the reply, which put
    /// a device on the end of a wifi link in front of every other statistic in the cycle - and one
    /// failed reading stopped the cycle for good, because the worker's retry consumed the command
    /// without answering it and the monitor sat waiting for a reply only a command it would never
    /// send could produce. A cycle now takes what has arrived and carries on; a plug that is slow
    /// or gone leaves the energy figure standing still instead of freezing the whole page.
    ///
    /// `None` means no reading arrived since the last call, not that anything is wrong.
    pub fn poll_energy(&self) -> Option<EnergyResult> {
        // Drain to the newest: with the worker answering on its own schedule there can be a
        // reading waiting that a later one has already superseded.
        let mut latest = None;
        while let Ok(Some(result)) = self.data_rx.try_recv() {
            latest = Some(result);
        }

        // A full queue means the worker has a request in hand already, which is as good as
        // sending another one.
        _ = self.command_tx.try_send(());

        latest
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
            let mut device = None;

            let mut commands = command_rx.into_stream();
            while let Some(()) = commands.next().await {
                // Every command is answered exactly once. The retry happens here rather than by
                // waiting for the next command: a plug that has dropped its session fails the
                // first call and answers a freshly handshaked one fine, and either way the
                // caller is owed a reply for the command it sent.
                let mut result = fetch_energy(&mut device, &ip, &email, &password).await;
                if result.is_err() {
                    result = fetch_energy(&mut device, &ip, &email, &password).await;
                }

                let failed = result.is_err();
                if data_tx.send_async(result).await.is_err() {
                    break;
                }

                if failed {
                    tokio::time::sleep(RETRY_BACKOFF).await;
                }
            }
        })
}

/// One reading, taken on the session we are holding or on a fresh one.
///
/// The session is kept only while it works: a call that fails drops it, so the next attempt
/// handshakes again instead of retrying against a session the plug has already forgotten.
async fn fetch_energy(
    device: &mut Option<tapo::PlugEnergyMonitoringHandler>,
    ip: &str,
    email: &str,
    password: &str,
) -> EnergyResult {
    let handler = match device.take() {
        Some(handler) => handler,
        None => tapo::ApiClient::new(email, password).p110(ip).await?,
    };

    let result = handler.get_energy_usage().await;
    if result.is_ok() {
        *device = Some(handler);
    }
    result
}
