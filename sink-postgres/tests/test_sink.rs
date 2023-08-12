use std::sync::Arc;

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::Sink;
use apibara_sink_postgres::{PostgresClient, PostgresSink, SinkPostgresError};
use async_trait::async_trait;
use futures_util::FutureExt;
use mockall::predicate;
use serde_json::{json, Map, Value};
use tokio::net::TcpStream;
use tokio_postgres::config::SslMode;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::types::{Json, ToSql};
use tokio_postgres::{Client, Config, Connection, NoTls, Statement};
// use futures_channel::mpsc;

struct MockPostgresClient<'a, 'b, 'c, 'd> {
    prepare_return: Result<Statement, tokio_postgres::Error>,
    pub prepare_calls: std::collections::HashMap<&'a str, Result<Statement, tokio_postgres::Error>>,
    pub execute_calls: std::collections::HashMap<
        (&'b Statement, &'c [&'d (dyn ToSql + Sync)]),
        Result<u64, tokio_postgres::Error>,
    >,
}

#[async_trait]
impl PostgresClient for MockPostgresClient<'_, '_, '_, '_> {
    async fn prepare(&self, query: &str) -> Result<Statement, tokio_postgres::Error> {
        self.prepare_calls.insert(query, self.prepare_return);
        self.prepare_return
    }

    async fn execute(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        todo!()
    }
}

fn new_batch(size: usize) -> Value {
    let mut batch = Vec::new();
    for i in 0..size {
        batch.push(json!({
            "batchNum": i,
            "batchStr": format!("batch_{}", i),
        }));
    }
    json!(batch)
}

fn new_rows(size: usize, block_number: u64) -> Json<Vec<Map<String, Value>>> {
    let mut batch = Vec::new();
    for i in 0..size {
        batch.push(
            json!({
                "batchNum": i,
                "batchStr": format!("batch_{}", i),
                "_cursor": block_number,
            })
            .as_object()
            .unwrap()
            .to_owned(),
        );
    }
    Json(batch)
}

// #[tokio::test]
// async fn test_handle_data() -> Result<(), SinkPostgresError> {
//     let table_name = "test";
//     let mut mock = MockPostgresClient::new();

//     let insert_query =
//         "INSERT INTO test SELECT * FROM json_populate_recordset(NULL::test, $1::json)";
//     let delete_query = "DELETE FROM test WHERE _cursor > $1";
//     let delete_all_query = "DELETE FROM test";

//     let (insert_statement, delete_statement, delete_all_statement) =
//         PostgresSink::get_statements(&mock, &table_name).await?;

//     for i in 0..5 {
//         let end_cursor = new_cursor(i + 1);
//         let rows = new_rows(1, end_cursor.order_key);
//     }

//     let mut sink = PostgresSink::new(Box::new(mock));

//     for i in 0..5 {
//         let cursor = Some(new_cursor(i));
//         let end_cursor = new_cursor(i + 1);
//         let finality = DataFinality::DataStatusFinalized;
//         let batch = new_batch(1);

//         sink.handle_data(&cursor, &end_cursor, &finality, &batch)
//             .await?;
//     }

//     Ok(())
// }

// fn new_cursor(order_key: u64) -> Cursor {
//     Cursor {
//         order_key,
//         unique_key: order_key.to_be_bytes().to_vec(),
//     }
// }

// async fn connect_raw(
//     s: &str,
// ) -> Result<(Client, Connection<TcpStream, NoTlsStream>), tokio_postgres::Error> {
//     let socket = TcpStream::connect("127.0.0.1:5433").await.unwrap();
//     let config = s.parse::<Config>().unwrap();
//     config.connect_raw(socket, NoTls).await
// }

// async fn connect(s: &str) -> Client {
//     let (client, connection) = connect_raw(s).await.unwrap();
//     let connection = connection.map(|r| r.unwrap());
//     tokio::spawn(connection);
//     client
// }

// async fn new_client() -> Client {
//     let (sender, receiver) = mpsc::unbounded();
//     // let client = Client {sender, config.ssl_mode, process_id, secret_key}
//     Client {
//         inner: Arc::new(InnerClient {
//             sender,
//             cached_typeinfo: Default::default(),
//             buffer: Default::default(),
//         }),
//         socket_config: None,
//         ssl_mode: SslMode::Disable,
//         process_id: 0,
//         secret_key: 0,
//     }
// }

#[tokio::test]
async fn test_prepare() -> Result<(), apibara_sink_postgres::SinkPostgresError> {
    let table_name = "test";
    let mut mock = MockPostgresClient::new();

    let insert_query =
        "INSERT INTO test SELECT * FROM json_populate_recordset(NULL::test, $1::json)";
    let delete_query = "DELETE FROM test WHERE _cursor > $1";
    let delete_all_query = "DELETE FROM test";

    mock.expect_prepare()
        .with(predicate::eq(insert_query))
        .times(1);

    mock.expect_prepare()
        .with(predicate::eq(delete_query))
        .times(1);

    mock.expect_prepare()
        .with(predicate::eq(delete_all_query))
        .times(1);

    let (insert_statement, delete_statement, delete_all_statement) =
        PostgresSink::get_statements(&mock, &table_name).await?;

    Ok(())
}

// #[tokio::test]
// async fn test_handle_invalidate() -> Result<(), apibara_sink_mongo::SinkMongoError> {
//     let mut mock = MockMongoCollection::<Document>::new();

//     for i in 0..5 {
//         let cursor = Some(new_cursor(i));

//         let query = doc! {"_cursor": {"$gt": cursor.map(|c| c.order_key as u32).unwrap_or(0)}};

//         mock.expect_delete_many()
//             .with(predicate::eq(query), predicate::always())
//             .times(1)
//             .return_const(Ok(1));
//     }

//     let mut sink = MongoSink::new(Box::new(mock));

//     for i in 0..5 {
//         let cursor = Some(new_cursor(i));
//         sink.handle_invalidate(&cursor).await?;
//     }

//     Ok(())
// }
