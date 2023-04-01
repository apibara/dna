use std::sync::Arc;

use apibara_node::db::libmdbx::{Environment, EnvironmentKind};

pub struct ErigonDB<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
}

impl<E> ErigonDB<E>
where
    E: EnvironmentKind,
{
    pub fn new() -> Self {
        todo!()
    }
}
