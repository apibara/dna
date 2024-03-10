use apibara_core::node::v1alpha2::DataFinality;
use apibara_sink_common::{Context, Sink, SinkError};
use apibara_sink_postgres::PostgresSink;
use async_trait::async_trait;
use error_stack::Result;
use serde_json::json;
use testcontainers::clients;
use tokio_postgres::NoTls;

mod common;
use crate::common::*;

#[derive(Debug, PartialEq)]
struct TestRow {
    pub id: String,
    pub counter: i64,
}

async fn create_test_table(port: u16) {
    let create_table_query = "CREATE TABLE test(id TEXT, counter bigint, _cursor int8range)";

    let connection_string = format!("postgresql://postgres@localhost:{}", port);
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.query(create_table_query, &[]).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_entity_mode() -> Result<(), SinkError> {
    let docker = clients::Cli::default();
    let postgres = docker.run(new_postgres_image());
    let port = postgres.get_host_port_ipv4(5432);

    create_test_table(port).await;

    let finality = DataFinality::DataStatusAccepted;

    let mut sink = new_entity_mode_sink(port).await;
    assert_eq!(0, sink.all_rows(0).await.len());

    // insert 2 entities at block 0
    {
        let ctx = Context {
            cursor: None,
            end_cursor: new_cursor(0),
            finality,
        };

        let batch = json!([
            {
                "insert": {
                    "id": "A",
                    "counter": 1,
                },
            },
            {
                "insert": {
                    "id": "B",
                    "counter": 1,
                },
            },
        ]);

        sink.handle_data(&ctx, &batch).await?;

        assert_eq!(2, sink.all_rows(0).await.len());
    }

    // insert one entity and update A
    {
        let ctx = Context {
            cursor: Some(new_cursor(0)),
            end_cursor: new_cursor(1),
            finality,
        };

        let batch = json!([
            {
                "insert": {
                    "id": "C",
                    "counter": 1,
                },
            },
            {
                "entity": {
                    "id": "A",
                },
                "update": {
                    "counter": 2,
                },
            },
        ]);

        sink.handle_data(&ctx, &batch).await?;

        let rows = sink.all_rows(1).await;
        assert_eq!(3, rows.len());
        assert_eq!(2, rows[0].counter);
        assert_eq!(1, rows[1].counter);
        assert_eq!(1, rows[2].counter);

        // previous values are still there
        let rows = sink.all_rows(0).await;
        assert_eq!(2, rows.len());
        assert_eq!(1, rows[0].counter);
        assert_eq!(1, rows[1].counter);
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_entity_mode_invalidate_genesis() -> Result<(), SinkError> {
    let docker = clients::Cli::default();
    let postgres = docker.run(new_postgres_image());
    let port = postgres.get_host_port_ipv4(5432);

    create_test_table(port).await;

    let finality = DataFinality::DataStatusAccepted;

    let mut sink = new_entity_mode_sink(port).await;
    assert_eq!(0, sink.all_rows(0).await.len());

    let ctx = Context {
        cursor: None,
        end_cursor: new_cursor(0),
        finality,
    };

    let batch = json!([
        {
            "insert": {
                "id": "A",
                "counter": 1,
            },
        },
        {
            "insert": {
                "id": "B",
                "counter": 1,
            },
        },
    ]);

    sink.handle_data(&ctx, &batch).await?;
    assert_eq!(2, sink.all_rows(0).await.len());

    sink.handle_invalidate(&None).await?;
    assert_eq!(0, sink.all_rows(0).await.len());

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_entity_mode_invalidate() -> Result<(), SinkError> {
    let docker = clients::Cli::default();
    let postgres = docker.run(new_postgres_image());
    let port = postgres.get_host_port_ipv4(5432);

    create_test_table(port).await;

    let finality = DataFinality::DataStatusAccepted;

    let mut sink = new_entity_mode_sink(port).await;
    assert_eq!(0, sink.all_rows(0).await.len());

    {
        let ctx = Context {
            cursor: None,
            end_cursor: new_cursor(0),
            finality,
        };

        let batch = json!([
            {
                "insert": {
                    "id": "A",
                    "counter": 1,
                },
            },
            {
                "insert": {
                    "id": "B",
                    "counter": 1,
                },
            },
        ]);

        sink.handle_data(&ctx, &batch).await?;
        assert_eq!(2, sink.all_rows(0).await.len());
    }

    // add and update entities at later blocks
    {
        let ctx = Context {
            cursor: Some(new_cursor(9)),
            end_cursor: new_cursor(10),
            finality,
        };

        let batch = json!([
            {
                "insert": {
                    "id": "C",
                    "counter": 1,
                },
            },
            {
                "entity": {
                    "id": "A",
                },
                "update": {
                    "counter": 2,
                },
            },
        ]);

        sink.handle_data(&ctx, &batch).await?;
        assert_eq!(3, sink.all_rows(15).await.len());
    }

    // A chain reorg will remove entity C and rollback entity A
    sink.handle_invalidate(&Some(new_cursor(9))).await?;

    let rows = sink.all_rows(15).await;
    assert_eq!(2, rows.len());

    assert_eq!(1, rows[0].counter);
    assert_eq!("A", &rows[0].id);
    assert_eq!(1, rows[1].counter);
    assert_eq!("B", &rows[1].id);

    Ok(())
}

#[async_trait]
trait SinkExt {
    async fn all_rows(&self, at_block: i64) -> Vec<TestRow>;
}

#[async_trait]
impl SinkExt for PostgresSink {
    async fn all_rows(&self, at_block: i64) -> Vec<TestRow> {
        let client = self.client();
        let rows = client
            .query(
                "SELECT * FROM test WHERE $1::bigint <@ _cursor ORDER BY id ASC",
                &[&at_block],
            )
            .await
            .unwrap();

        rows.into_iter()
            .map(|row| {
                let id: String = row.get("id");
                let counter: i64 = row.get("counter");
                TestRow { id, counter }
            })
            .collect()
    }
}
