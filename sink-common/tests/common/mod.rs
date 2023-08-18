use std::collections::HashMap;

use testcontainers::{core::WaitFor, Image};

#[derive(Clone, Debug)]
pub struct Etcd {
    env_vars: HashMap<String, String>,
}

impl Default for Etcd {
    fn default() -> Self {
        let mut env_vars = HashMap::new();

        env_vars.insert("ALLOW_NONE_AUTHENTICATION".to_string(), "yes".to_string());

        Self { env_vars }
    }
}

impl Image for Etcd {
    type Args = ();

    fn name(&self) -> String {
        "bitnami/etcd".to_string()
    }

    fn tag(&self) -> String {
        "3.5".to_string()
    }

    fn env_vars(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        Box::new(self.env_vars.iter())
    }

    fn ready_conditions(&self) -> Vec<testcontainers::core::WaitFor> {
        vec![WaitFor::message_on_stderr("ready to serve client requests")]
    }

    fn expose_ports(&self) -> Vec<u16> {
        vec![2379]
    }
}
