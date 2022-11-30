//! Check health of the node.

use std::{sync::Arc, time::Duration};

use apibara_core::node::v1alpha1::pb;
use apibara_node::db::{
    libmdbx::{Environment, EnvironmentKind, Error as MdbxError},
    tables, MdbxTransactionExt,
};
use tokio_util::sync::CancellationToken;
use tonic_health::proto::health_server::{Health, HealthServer};
use tracing::warn;

use crate::{core::Block, server::NodeServer};

pub struct HealthReporter<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
    reporter: tonic_health::server::HealthReporter,
}

impl<E> HealthReporter<E>
where
    E: EnvironmentKind,
{
    pub fn new(db: Arc<Environment<E>>) -> (Self, HealthServer<impl Health>) {
        let (reporter, service) = tonic_health::server::health_reporter();
        (HealthReporter { db, reporter }, service)
    }

    pub async fn start(&mut self, ct: CancellationToken) {
        let interval = Duration::from_secs(1);
        loop {
            if ct.is_cancelled() {
                return;
            }

            if self.check_db().is_ok() {
                self.set_serving().await;
            } else {
                self.set_not_serving().await;
            }

            tokio::time::sleep(interval).await;
        }
    }

    fn check_db(&self) -> Result<(), MdbxError> {
        let txn = self.db.begin_ro_txn()?;
        // access one table to see if db access is working
        let mut cursor = txn.open_table::<tables::BlockTable<Block>>()?.cursor()?;
        cursor.last()?;
        txn.commit()?;
        Ok(())
    }

    async fn set_serving(&mut self) {
        self.reporter
            .set_serving::<pb::node_server::NodeServer<NodeServer<E>>>()
            .await
    }

    async fn set_not_serving(&mut self) {
        warn!("server is not serving");
        self.reporter
            .set_not_serving::<pb::node_server::NodeServer<NodeServer<E>>>()
            .await
    }
}
