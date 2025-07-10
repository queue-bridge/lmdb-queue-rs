use std::path::Path;
use lmdb::{Environment, RoTransaction, RwTransaction, EnvironmentFlags, Error};
use libc::{c_uint, size_t};

pub struct Env {
    lmdb_env: lmdb::Environment,
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

    pub fn topic(&self, name: &str) -> Result<super::topic::Topic, Error> {
        super::topic::Topic::new(&self, name)
    }

    pub fn transaction_ro(&self) -> Result<RoTransaction, Error> {
        self.lmdb_env.begin_ro_txn()
    }

    pub fn transaction_rw(&self) -> Result<RwTransaction, Error> {
        self.lmdb_env.begin_rw_txn()
    }
}

#[test]
fn test_env_new() -> Result<(), Error> {
    let env = Env::new("/tmp/foo", None, None)?;
    let topic = env.topic("bar")?;
    let head = topic.get_producer_head()?;
    assert_eq!(head, 0);
    let head_file = topic.get_producer_head_file()?;
    assert_eq!(head_file, 0);

    Ok(())
}