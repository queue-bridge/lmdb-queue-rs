use std::path::Path;
use lmdb::{Environment, RoTransaction, RwTransaction, EnvironmentFlags, Error};
use libc::{c_uint, size_t};

pub struct Env {
    pub lmdb_env: lmdb::Environment,
    pub root: String,
}

impl Env {
    pub fn new<P: AsRef<Path>>(root: P, max_topics: Option<c_uint>, map_size: Option<size_t>) -> Result<Env, Error> {
        Environment::new()
            .set_map_size(map_size.unwrap_or(256 * 1024 * 1024))
            .set_max_dbs(max_topics.unwrap_or(256))
            .set_flags(EnvironmentFlags::NO_SYNC | EnvironmentFlags::NO_SUB_DIR)
            .open(root.as_ref())
            .map(|lmdb_env| Env { lmdb_env, root: root.as_ref().to_str().unwrap().to_string() })
    }

    pub fn producer(&self, name: &str) -> Result<super::topic::Producer, anyhow::Error> {
        super::topic::Producer::new(&self, name)
    }

    pub fn comsumer(&self, name: &str) -> Result<super::topic::Comsumer, anyhow::Error> {
        super::topic::Comsumer::new(&self, name)
    }

    pub fn transaction_ro(&self) -> Result<RoTransaction, Error> {
        self.lmdb_env.begin_ro_txn()
    }

    pub fn transaction_rw(&self) -> Result<RwTransaction, Error> {
        self.lmdb_env.begin_rw_txn()
    }
}

#[test]
fn test_env_new() -> Result<(), anyhow::Error> {
    let env = Env::new("/tmp/foo_env", None, None)?;
    let producer = env.producer("test")?;
    let comsumer = env.comsumer("test")?;

    Ok(())
}