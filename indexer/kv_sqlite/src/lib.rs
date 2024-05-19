use std::fmt;

use error_stack::{Result, ResultExt};
use indexer_core::{Context, KeyValueStorage, KeyValueStorageError};
use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;

const MIGRATIONS: &[&str] = &[r#"
    CREATE TABLE IF NOT EXISTS key_values (
        key TEXT,
        value BLOB,
        valid_from INTEGER,
        valid_to INTEGER,
        UNIQUE(key, valid_from)
    );
    "#];

#[derive(Debug)]
pub struct SqliteError;

pub struct SqliteKeyValueStorage {
    connection: Connection,
}

impl SqliteKeyValueStorage {
    pub fn with_connection(connection: Connection) -> Self {
        Self { connection }
    }

    pub fn initialize(&self) -> Result<(), SqliteError> {
        for migration in MIGRATIONS {
            self.connection
                .execute(migration, rusqlite::params![])
                .change_context(SqliteError)
                .attach_printable("failed to apply migration to database")?;
        }

        Ok(())
    }
}

impl KeyValueStorage for SqliteKeyValueStorage {
    fn get(
        &mut self,
        _ctx: &Context,
        key: impl AsRef<str>,
    ) -> Result<Option<Value>, KeyValueStorageError> {
        let value = self
            .connection
            .query_row(
                "SELECT value FROM key_values where key = ? AND valid_to IS NULL",
                [key.as_ref()],
                |row| {
                    let value: String = row.get(0)?;
                    Ok(value)
                },
            )
            .optional()
            .change_context(SqliteError)
            .attach_printable("faliure to get value from database")
            .change_context(KeyValueStorageError)?;

        let Some(value) = value else {
            return Ok(None);
        };

        let value = serde_json::from_str(&value)
            .change_context(SqliteError)
            .attach_printable("failed to parse value")
            .change_context(KeyValueStorageError)?;

        Ok(Some(value))
    }

    fn set(
        &mut self,
        ctx: &Context,
        key: impl AsRef<str>,
        value: &Value,
    ) -> Result<(), KeyValueStorageError> {
        let serialized = serde_json::to_string(value)
            .change_context(SqliteError)
            .attach_printable("failed to serialize value")
            .change_context(KeyValueStorageError)?;

        let tx = self
            .connection
            .transaction()
            .change_context(SqliteError)
            .attach_printable("failed to create transaction")
            .change_context(KeyValueStorageError)?;

        // First update any existing value
        tx.execute(
            "UPDATE key_values SET valid_to = ?1 WHERE key = ?2 AND valid_to IS NULL AND valid_from < ?1",
            (ctx.block_number, key.as_ref()),
        )
        .change_context(SqliteError)
        .attach_printable("failed to clamp existing value")
        .change_context(KeyValueStorageError)?;

        // Then insert the new value
        tx.execute(
            r#"
            INSERT INTO key_values(valid_from, key, value)
            VALUES(?1, ?2, ?3)
            ON CONFLICT (key, valid_from) DO UPDATE SET value = excluded.value"#,
            (ctx.block_number, key.as_ref(), &serialized),
        )
        .change_context(SqliteError)
        .attach_printable("failed to insert new value")
        .change_context(KeyValueStorageError)?;

        tx.commit()
            .change_context(SqliteError)
            .attach_printable("failed to commit changes")
            .change_context(KeyValueStorageError)?;

        Ok(())
    }

    fn del(&mut self, ctx: &Context, key: impl AsRef<str>) -> Result<(), KeyValueStorageError> {
        self.connection
            .execute(
                "UPDATE key_values SET valid_to = ?1 WHERE key = ?2 AND valid_to IS NULL",
                (ctx.block_number, key.as_ref()),
            )
            .change_context(SqliteError)
            .attach_printable("failed to (soft) delete value")
            .change_context(KeyValueStorageError)?;

        Ok(())
    }

    fn invalidate(&mut self, block_number: u64) -> Result<(), KeyValueStorageError> {
        let tx = self
            .connection
            .transaction()
            .change_context(SqliteError)
            .attach_printable("failed to create transaction")
            .change_context(KeyValueStorageError)?;

        // First remove any values that have been inserted _at or after_ the current block
        tx.execute(
            "DELETE FROM key_values WHERE valid_from >= ?",
            [block_number],
        )
        .change_context(SqliteError)
        .attach_printable("failed to invalidate values")
        .change_context(KeyValueStorageError)?;

        // Then unclamp any values that have been clamped _at or after_ the current block
        tx.execute(
            "UPDATE key_values SET valid_to = NULL WHERE valid_to >= ?",
            [block_number],
        )
        .change_context(SqliteError)
        .attach_printable("failed to unclamp values")
        .change_context(KeyValueStorageError)?;

        tx.commit()
            .change_context(SqliteError)
            .attach_printable("failed to commit changes")
            .change_context(KeyValueStorageError)?;

        Ok(())
    }

    fn remove_finalized(&mut self, block_number: u64) -> Result<(), KeyValueStorageError> {
        self.connection
            .execute("DELETE FROM key_values WHERE valid_to < ?", [block_number])
            .change_context(SqliteError)
            .attach_printable("failed to remove finalized values")
            .change_context(KeyValueStorageError)?;

        Ok(())
    }
}

impl error_stack::Context for SqliteError {}

impl fmt::Display for SqliteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sqlite error")
    }
}

#[cfg(test)]
mod tests {
    use indexer_core::{Context, KeyValueStorage};
    use serde_json::json;

    use crate::SqliteKeyValueStorage;

    fn new_storage() -> SqliteKeyValueStorage {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        let storage = SqliteKeyValueStorage::with_connection(connection);
        storage.initialize().unwrap();
        storage
    }

    #[test]
    pub fn test_insert_and_get_at_same_block() {
        let mut storage = new_storage();
        let ctx = Context { block_number: 0 };

        {
            let value = storage.get(&ctx, "abc").unwrap();
            assert!(value.is_none());
        }

        {
            storage.set(&ctx, "abc", &json!({ "counter": 42 })).unwrap();
        }

        {
            let value = storage.get(&ctx, "abc").unwrap();
            assert_eq!(value.unwrap(), json!({ "counter": 42 }));
        }
    }

    #[test]
    pub fn test_multiple_insert_at_same_block() {
        let mut storage = new_storage();
        let ctx = Context { block_number: 0 };

        {
            storage.set(&ctx, "abc", &json!({ "counter": 42 })).unwrap();
            let value = storage.get(&ctx, "abc").unwrap();
            assert_eq!(value.unwrap(), json!({ "counter": 42 }));
        }

        {
            storage
                .set(&ctx, "abc", &json!({ "counter": 420 }))
                .unwrap();
            let value = storage.get(&ctx, "abc").unwrap();
            assert_eq!(value.unwrap(), json!({ "counter": 420 }));
        }
    }

    #[test]
    pub fn test_insert_and_get_at_different_blocks() {
        let mut storage = new_storage();

        {
            let ctx = Context { block_number: 0 };
            storage.set(&ctx, "abc", &json!({ "counter": 42 })).unwrap();
        }

        {
            let ctx = Context { block_number: 1 };
            let value = storage.get(&ctx, "abc").unwrap();
            assert_eq!(value.unwrap(), json!({ "counter": 42 }));
        }
    }

    #[test]
    pub fn test_delete_at_same_block() {
        let mut storage = new_storage();
        let ctx = Context { block_number: 0 };

        {
            storage.set(&ctx, "abc", &json!({ "counter": 42 })).unwrap();
        }

        {
            let value = storage.get(&ctx, "abc").unwrap();
            assert_eq!(value.unwrap(), json!({ "counter": 42 }));
        }

        {
            storage.del(&ctx, "abc").unwrap();
        }

        {
            let value = storage.get(&ctx, "abc").unwrap();
            assert!(value.is_none());
        }
    }

    #[test]
    pub fn test_delete_at_different_blocks() {
        let mut storage = new_storage();

        {
            let ctx = Context { block_number: 0 };
            storage.set(&ctx, "abc", &json!({ "counter": 42 })).unwrap();
        }

        {
            let ctx = Context { block_number: 0 };
            let value = storage.get(&ctx, "abc").unwrap();
            assert_eq!(value.unwrap(), json!({ "counter": 42 }));
        }

        {
            let ctx = Context { block_number: 1 };
            storage.del(&ctx, "abc").unwrap();
        }

        {
            let ctx = Context { block_number: 1 };
            let value = storage.get(&ctx, "abc").unwrap();
            assert!(value.is_none());
        }
    }

    #[test]
    pub fn test_invalidate() {
        let mut storage = new_storage();

        {
            let ctx = Context { block_number: 0 };
            storage.set(&ctx, "abc", &json!({ "counter": 42 })).unwrap();
            storage.set(&ctx, "xyz", &json!({ "counter": 0 })).unwrap();
        }

        {
            let ctx = Context { block_number: 10 };
            storage
                .set(&ctx, "abc", &json!({ "counter": 420 }))
                .unwrap();
        }

        {
            let ctx = Context { block_number: 10 };
            let value = storage.get(&ctx, "abc").unwrap();
            assert_eq!(value.unwrap(), json!({ "counter": 420 }));
            let value = storage.get(&ctx, "xyz").unwrap();
            assert_eq!(value.unwrap(), json!({ "counter": 0 }));
        }

        storage.invalidate(10).unwrap();

        {
            let ctx = Context { block_number: 10 };
            let value = storage.get(&ctx, "abc").unwrap();
            assert_eq!(value.unwrap(), json!({ "counter": 42 }));
            let value = storage.get(&ctx, "xyz").unwrap();
            assert_eq!(value.unwrap(), json!({ "counter": 0 }));
        }
    }

    #[test]
    pub fn test_remove_finalized() {
        let mut storage = new_storage();

        {
            let ctx = Context { block_number: 0 };
            storage.set(&ctx, "abc", &json!({ "counter": 42 })).unwrap();
        }

        {
            let ctx = Context { block_number: 10 };
            storage
                .set(&ctx, "abc", &json!({ "counter": 420 }))
                .unwrap();
        }

        {
            let ctx = Context { block_number: 20 };
            storage
                .set(&ctx, "abc", &json!({ "counter": 4200 }))
                .unwrap();
        }

        storage.remove_finalized(15).unwrap();

        {
            let ctx = Context { block_number: 20 };
            let value = storage.get(&ctx, "abc").unwrap();
            assert_eq!(value.unwrap(), json!({ "counter": 4200 }));
        }
    }
}
