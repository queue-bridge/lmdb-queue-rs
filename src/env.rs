use std::error::Error;
use std::path::Path;
use libc::{c_uint, size_t};

use heed3::{Database, EnvFlags, EnvOpenOptions, RoTxn, RwTxn, WithTls};

use super::topic::{Consumer, Producer};

#[cfg(test)]
use super::topic::Topic;

pub struct Env {
    pub lmdb_env: heed3::Env,
    pub root: String,
}

impl Env {
    pub fn new<P: AsRef<Path>>(root: P, max_topics: Option<c_uint>, map_size: Option<size_t>) -> Result<Env, Box<dyn Error>> {
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(map_size.unwrap_or(256 * 1024 * 1024))
                .max_dbs(max_topics.unwrap_or(256) * 2)
                .flags(EnvFlags::NO_SYNC | EnvFlags::NO_SUB_DIR)
                .open(root.as_ref())
                .map(|lmdb_env| Env { lmdb_env, root: root.as_ref().to_str().unwrap().to_string() })
        };

        Ok(env?)
    }

    pub fn db<K, V>(&self, wtxn: &mut RwTxn, name: &str) -> Result<Database<K, V>, Box<dyn Error>>
    where K: 'static, V: 'static
    {
        Ok(self.lmdb_env.open_database::<K, V>(wtxn, Some(name))?.unwrap())
    }

    pub fn producer(&self, name: &str, chunk_size: Option<u64>) -> Result<Producer, Box<dyn Error>> {
        Producer::new(&self, name, chunk_size)
    }

    pub fn consumer(&self, name: &str, chunks_to_keep: Option<u64>) -> Result<Consumer, Box<dyn Error>> {
        Consumer::new(&self, name, chunks_to_keep)
    }

    pub fn write_txn(&self) -> Result<RwTxn, Box<dyn Error>> {
        Ok(self.lmdb_env.write_txn()?)
    }

    pub fn read_txn(&self) -> Result<RoTxn<WithTls>, Box<dyn Error>> {
        Ok(self.lmdb_env.read_txn()?)
    }
}

#[test]
fn test_single() -> Result<(), Box<dyn Error>> {
    let env = Env::new("/tmp/foo_env", None, None)?;
    let mut producer = env.producer("test", Some(16 *1024 * 1024))?;
    for i in 0..1024*1024 {
        producer.push_back(&format!("{}", i).as_bytes())?;
    }

    let mut consumer = env.consumer("test", None)?;
    let lag = consumer.lag()?;
    println!("Current lag is: {}", lag);

    let mut message_count = 0;
    loop {
        let item = consumer.pop_front()?;
        if let Some(item) = item {
            message_count += 1;
            if message_count % (1024 * 100) == 0 {
                println!("Got message: {}", String::from_utf8(item.data)?);
                let cur_lag = consumer.lag()?;
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
fn test_batch() -> Result<(), Box<dyn Error>> {
    let env = Env::new("/tmp/foo_env", None, None)?;
    let mut producer = env.producer("test", Some(16 * 1024 * 1024))?;
    for i in 0..1024*100 {
        let vec: Vec<String> = (0..10).map(|v| format!("{}_{}", i, v)).collect();
        let batch: Vec<&[u8]> = vec.iter().map(|v| v.as_bytes()).collect();

        producer.push_back_batch(&batch)?;
    }

    let mut consumer = env.consumer("test", None)?;
    let lag = consumer.lag()?;
    println!("Current lag is: {}", lag);
    let mut message_count = 0;
    loop {
        let items = consumer.pop_front_n(10)?;
        if items.len() > 0 {
            message_count += items.len();
            if message_count % (1024 * 100) == 0 {
                println!("Got message: {}", String::from_utf8(items[0].data.clone())?);
                let cur_lag = consumer.lag()?;
                assert!(lag == cur_lag + message_count as u64);
            }
        } else {
            println!("Read {} messages.", message_count);
            break;
        }
    }

    Ok(())
}