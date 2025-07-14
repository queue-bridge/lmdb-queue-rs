use std::path::Path;
use lmdb::{Environment, RoTransaction, RwTransaction, EnvironmentFlags, Error};
use libc::{c_uint, size_t};

use super::topic::{Comsumer, Producer};

#[cfg(test)]
use super::topic::Topic;

pub struct Env {
    pub lmdb_env: lmdb::Environment,
    pub root: String,
}

impl Env {
    pub fn new<P: AsRef<Path>>(root: P, max_topics: Option<c_uint>, map_size: Option<size_t>) -> Result<Env, Error> {
        Environment::new()
            .set_map_size(map_size.unwrap_or(256 * 1024 * 1024))
            .set_max_dbs(max_topics.unwrap_or(256))
            .set_flags(EnvironmentFlags::NO_SYNC | EnvironmentFlags::NO_TLS | EnvironmentFlags::NO_SUB_DIR)
            .open(root.as_ref())
            .map(|lmdb_env| Env { lmdb_env, root: root.as_ref().to_str().unwrap().to_string() })
    }

    pub fn producer(&self, name: &str, chunk_size: Option<u64>) -> Result<super::topic::Producer, anyhow::Error> {
        Producer::new(&self, name, chunk_size)
    }

    pub fn comsumer(&self, name: &str, chunks_to_keep: Option<u64>) -> Result<super::topic::Comsumer, anyhow::Error> {
        Comsumer::new(&self, name, chunks_to_keep)
    }

    pub fn transaction_ro(&self) -> Result<RoTransaction, Error> {
        self.lmdb_env.begin_ro_txn()
    }

    pub fn transaction_rw(&self) -> Result<RwTransaction, Error> {
        self.lmdb_env.begin_rw_txn()
    }
}

#[test]
fn test_single() -> Result<(), anyhow::Error> {
    let env = Env::new("/tmp/foo_env", None, None)?;
    let mut producer = env.producer("test", Some(16 *1024 * 1024))?;
    for i in 0..1024*1024 {
        producer.push_back(&format!("{}", i).as_bytes())?;
    }

    let mut comsumer = env.comsumer("test", None)?;
    let lag = comsumer.lag()?;
    println!("Current lag is: {}", lag);

    let mut message_count = 0;
    loop {
        let item = comsumer.pop_front()?;
        if let Some(item) = item {
            message_count += 1;
            if message_count % (1024 * 100) == 0 {
                println!("Got message: {}", String::from_utf8(item.data)?);
                let cur_lag = comsumer.lag()?;
                assert!(lag == cur_lag + message_count as u64);
            }
        } else {
            println!("Read {} messages.", message_count);
            break;
        }
    }

    Ok(())
}

#[test]
fn test_batch() -> Result<(), anyhow::Error> {
    let env = Env::new("/tmp/foo_env", None, None)?;
    let mut producer = env.producer("test", Some(16 * 1024 * 1024))?;
    for i in 0..1024*100 {
        let vec: Vec<String> = (0..10).map(|v| format!("{}_{}", i, v)).collect();
        let batch: Vec<&[u8]> = vec.iter().map(|v| v.as_bytes()).collect();

        producer.push_back_batch(&batch)?;
    }

    let mut comsumer = env.comsumer("test", None)?;
    let lag = comsumer.lag()?;
    println!("Current lag is: {}", lag);
    let mut message_count = 0;
    loop {
        let items = comsumer.pop_front_n(10)?;
        if items.len() > 0 {
            message_count += items.len();
            if message_count % (1024 * 100) == 0 {
                println!("Got message: {}", String::from_utf8(items[0].data.clone())?);
                let cur_lag = comsumer.lag()?;
                assert!(lag == cur_lag + message_count as u64);
            }
        } else {
            println!("Read {} messages.", message_count);
            break;
        }
    }

    Ok(())
}