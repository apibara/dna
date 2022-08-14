use std::{marker::PhantomData, path::Path};

use libmdbx::{
    Cursor, Database, DatabaseFlags, Environment, EnvironmentKind, Error as MdbxError, TableObject,
    Transaction, TransactionKind, WriteFlags, RW,
};
use prost::Message;

use super::{
    table::{Table, TableKey},
    DupSortTable,
};

/// A type-safe view over a mdbx database.
pub struct MdbxTable<'txn, T, K, E>
where
    T: Table,
    K: TransactionKind,
    E: EnvironmentKind,
{
    txn: &'txn Transaction<'txn, K, E>,
    db: Database<'txn>,
    phantom: PhantomData<T>,
}

/// A cursor over items in a `MdbxTable`.
pub struct TableCursor<'txn, T, K>
where
    T: Table,
    K: TransactionKind,
{
    cursor: Cursor<'txn, K>,
    phantom: PhantomData<T>,
}

/// Extension methods over mdbx environment.
pub trait MdbxEnvironmentExt<E: EnvironmentKind> {
    /// Open and configure a mdbx environment.
    fn open(path: &Path) -> Result<Environment<E>, MdbxError>;
}

/// Extension methods over mdbx RO and RW transactions.
pub trait MdbxTransactionExt<K: TransactionKind, E: EnvironmentKind> {
    /// Open a database accessed through a type-safe [MdbxTable].
    fn open_table<'txn, T: Table>(&'txn self) -> Result<MdbxTable<'txn, T, K, E>, MdbxError>;
}

/// Extension methods over mdbx RW transactions.
pub trait MdbxRWTransactionExt {
    /// Ensure the given table database exists. Creates it if it doesn't.
    fn ensure_table<T: Table>(&self, flags: Option<DatabaseFlags>) -> Result<(), MdbxError>;
}

impl<E: EnvironmentKind> MdbxEnvironmentExt<E> for Environment<E> {
    fn open(path: &Path) -> Result<Environment<E>, MdbxError> {
        let mut builder = Environment::new();
        builder.set_max_dbs(16);
        builder.open(path)
    }
}

impl<'env, K, E> MdbxTransactionExt<K, E> for Transaction<'env, K, E>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    fn open_table<'txn, T: Table>(&'txn self) -> Result<MdbxTable<'txn, T, K, E>, MdbxError> {
        let database = self.open_db(Some(T::db_name()))?;
        Ok(MdbxTable {
            txn: self,
            db: database,
            phantom: Default::default(),
        })
    }
}

impl<'env, E: EnvironmentKind> MdbxRWTransactionExt for Transaction<'env, RW, E> {
    fn ensure_table<T: Table>(&self, flags: Option<DatabaseFlags>) -> Result<(), MdbxError> {
        let flags = flags.unwrap_or_default();
        let name = T::db_name();
        self.create_db(Some(name), flags)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct TableObjectWrapper<T>(T);

impl<'txn, T> TableObject<'txn> for TableObjectWrapper<T>
where
    T: Message + Default,
{
    fn decode(data_val: &[u8]) -> Result<Self, MdbxError>
    where
        Self: Sized,
    {
        T::decode(data_val)
            .map_err(|err| MdbxError::DecodeError(Box::new(err)))
            .map(Self)
    }
}

#[derive(Debug, Clone)]
struct TableKeyWrapper<T>(T);

impl<'txn, T> TableObject<'txn> for TableKeyWrapper<T>
where
    T: TableKey,
{
    fn decode(data_val: &[u8]) -> Result<Self, MdbxError>
    where
        Self: Sized,
    {
        T::decode(data_val)
            .map_err(|err| MdbxError::DecodeError(Box::new(err)))
            .map(Self)
    }
}

impl<'txn, T, K, E> MdbxTable<'txn, T, K, E>
where
    T: Table,
    K: TransactionKind,
    E: EnvironmentKind,
{
    /// Returns a cursor over the items in the table.
    pub fn cursor(&self) -> Result<TableCursor<'txn, T, K>, MdbxError> {
        let cursor = self.txn.cursor(&self.db)?;
        Ok(TableCursor {
            cursor,
            phantom: Default::default(),
        })
    }

    /// Get an item in the table by its `key`.
    pub fn get(&self, key: &T::Key) -> Result<Option<T::Value>, MdbxError> {
        let data = self
            .txn
            .get::<TableObjectWrapper<_>>(&self.db, key.encode().as_ref())?;
        Ok(data.map(|d| d.0))
    }
}

impl<'txn, T, K> TableCursor<'txn, T, K>
where
    T: Table,
    K: TransactionKind,
{
    /// Position at the first item.
    pub fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, MdbxError> {
        map_kv_result::<T>(self.cursor.first())
    }

    /// Position at the last item.
    pub fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, MdbxError> {
        map_kv_result::<T>(self.cursor.last())
    }

    /// Position at the specified key.
    pub fn seek_exact(&mut self, key: &T::Key) -> Result<Option<(T::Key, T::Value)>, MdbxError> {
        map_kv_result::<T>(self.cursor.set_key(key.encode().as_ref()))
    }

    /// Position at the first key greater than or equal to the specified key.
    pub fn seek_range(&mut self, key: &T::Key) -> Result<Option<(T::Key, T::Value)>, MdbxError> {
        map_kv_result::<T>(self.cursor.set_range(key.encode().as_ref()))
    }
}

impl<'txn, T, K> TableCursor<'txn, T, K>
where
    T: DupSortTable,
    K: TransactionKind,
{
    pub fn next_dup(&mut self) -> Result<Option<(T::Key, T::Value)>, MdbxError> {
        map_kv_result::<T>(self.cursor.next_dup())
    }
}

impl<'txn, T> TableCursor<'txn, T, RW>
where
    T: Table,
{
    pub fn put(&mut self, key: &T::Key, value: &T::Value) -> Result<(), MdbxError> {
        let data = T::Value::encode_to_vec(&value);
        self.cursor
            .put(key.encode().as_ref(), &data, WriteFlags::default())?;
        Ok(())
    }
}

impl<'txn, T> TableCursor<'txn, T, RW>
where
    T: DupSortTable,
{
    pub fn append_dup(&mut self, key: &T::Key, value: &T::Value) -> Result<(), MdbxError> {
        let data = T::Value::encode_to_vec(&value);
        self.cursor
            .put(key.encode().as_ref(), &data, WriteFlags::APPEND_DUP)?;
        Ok(())
    }
}

fn map_kv_result<T>(
    t: Result<Option<(TableKeyWrapper<T::Key>, TableObjectWrapper<T::Value>)>, MdbxError>,
) -> Result<Option<(T::Key, T::Value)>, MdbxError>
where
    T: Table,
{
    if let Some((k, v)) = t? {
        return Ok(Some((k.0, v.0)));
    }
    Ok(None)
}
