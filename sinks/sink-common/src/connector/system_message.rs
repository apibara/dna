use apibara_dna_protocol::dna::stream::{system_message::Output, SystemMessage};
use tracing::{info, warn};

pub fn log_system_message(message: SystemMessage) {
    let Some(output) = message.output else {
        return;
    };

    match output {
        Output::Stdout(message) => {
            info!("system message: {}", message);
        }
        Output::Stderr(message) => {
            warn!("system message: {}", message);
        }
    }
}
