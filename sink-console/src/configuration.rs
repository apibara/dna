use apibara_sink_common::SinkOptions;
use clap::Args;

#[derive(Debug, Args, Default, SinkOptions)]
#[sink_options(tag = "console")]
pub struct SinkConsoleOptions {}

impl SinkOptions for SinkConsoleOptions {
    fn merge(self, _other: SinkConsoleOptions) -> Self {
        SinkConsoleOptions::default()
    }
}
