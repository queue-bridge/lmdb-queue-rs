use std::error::Error;
use heed3::byteorder::BE;
use heed3::types::*;
use heed3::{RwTxn, Database, PutFlags};

use super::env::Env;

use super::reader::{Reader, Item};
use super::writer::Writer;

pub static KEY_CONSUMER_FILE: &str = "FILE";
pub static KEY_CONSUMER_OFFSET: &str = "OFFSET";
pub static KEY_CONSUMER_BYTES_READ: &str = "BYTES_READ";

pub trait Topic {
    fn get_env(&self) -> &Env;
    fn get_producer_db(&self) -> Database<U64<BE>, U64<BE>>;
    fn get_consumer_db(&self) -> Database<Str, U64<BE>>;

    fn lag(&self) -> Result<u64, Box<dyn Error>> {
        let txn = self.get_env().write_txn()?;

        let mut pit = self.get_producer_db().iter(&txn)?.move_between_keys();
        let mut total: u64 = 0;
        while let Some((_, v)) = pit.next().transpose()? {
            total += v;
        }

        let head_offset = self.get_consumer_db().get(&txn, KEY_CONSUMER_OFFSET)?.unwrap_or(0);
        Ok(total - head_offset)
    }
}

pub struct Producer<'env> {
    env: &'env Env,
    producer_db: Database<U64<BE>, U64<BE>>,
    consumer_db: Database<Str, U64<BE>>,
    writer: Writer,
    chunk_size: u64,
}

impl<'env> Topic for Producer<'env> {
    fn get_env(&self) -> &Env {
        self.env
    }

    fn get_producer_db(&self) -> Database<U64<BE>, U64<BE>> {
        self.producer_db
    }

    fn get_consumer_db(&self) -> Database<Str, U64<BE>> {
        self.consumer_db
    }
}

impl<'env> Producer<'env> {
    pub fn new(env: &'env Env, name: &str, chunk_size: Option<u64>) -> Result<Self, Box<dyn Error>> {
        let mut txn = env.write_txn()?;
        let producer_db: Database<U64<BE>, U64<BE>> = env.db(&mut txn, &format!("{}_{}", name, "producer"))?;
        let consumer_db: Database<Str, U64<BE>> = env.db(&mut txn, &format!("{}_{}", name, "consumer"))?;

        if let Ok(_) = consumer_db.put_with_flags(&mut txn, PutFlags::NO_OVERWRITE, &KEY_CONSUMER_FILE, &0) {
            producer_db.put_with_flags(&mut txn, PutFlags::NO_OVERWRITE, &0, &0)?;
            consumer_db.put_with_flags(&mut txn, PutFlags::NO_OVERWRITE, &KEY_CONSUMER_OFFSET, &0)?;
            consumer_db.put_with_flags(&mut txn, PutFlags::NO_OVERWRITE, &KEY_CONSUMER_BYTES_READ, &0)?;
        }

        let (tail_file, _) = producer_db.iter(&txn)?.last().transpose()?.unwrap();
        let writer = Writer::new(&env.root, name, tail_file)?;

        txn.commit()?;

        Ok(Producer { env, producer_db, consumer_db, writer, chunk_size: chunk_size.unwrap_or(64 * 1024 * 1024) })
    }

    pub fn push_back_batch<'a, B>(&mut self, messages: &'a B) -> Result<(), Box<dyn Error>>
    where B: AsRef<[&'a [u8]]>
    {
        let mut txn = self.env.write_txn()?;
        let (mut tail_file, mut offset) = self.producer_db.iter(&txn)?.last().transpose()?.unwrap();
        if tail_file > self.writer.get_file_num() {
            self.writer.rotate(Some(tail_file))?;
        }

        if self.writer.file_size()? > self.chunk_size {
            self.writer.rotate(None)?;
            tail_file += 1;
            offset = 0;
            self.producer_db.put(&mut txn, &tail_file, &0)?;
        }
        self.writer.put_batch(messages)?;
        self.producer_db.put(&mut txn, &tail_file, &(offset + messages.as_ref().len() as u64))?;
        txn.commit()?;
        Ok(())
    }

    pub fn push_back<'a>(&mut self, message: &'a [u8]) -> Result<(), Box<dyn Error>> {
        self.push_back_batch(&[message])
    }
}

pub struct Consumer<'env> {
    env: &'env Env,
    producer_db: Database<U64<BE>, U64<BE>>,
    consumer_db: Database<Str, U64<BE>>,
    reader: Reader,
    chunks_to_keep: u64,
}

impl <'env> Topic for Consumer<'env> {
    fn get_env(&self) -> &Env {
        self.env
    }

    fn get_producer_db(&self) -> Database<U64<BE>, U64<BE>> {
        self.producer_db
    }

    fn get_consumer_db(&self) -> Database<Str, U64<BE>> {
        self.consumer_db
    }
}

impl <'env> Consumer<'env> {
    pub fn new(env: &'env Env, name: &str, chunks_to_keep: Option<u64>) -> Result<Self, Box<dyn Error>> {
        let mut txn = env.write_txn()?;
        let producer_db: Database<U64<BE>, U64<BE>> = env.db(&mut txn, &format!("{}_{}", name, "producer"))?;
        let consumer_db: Database<Str, U64<BE>> = env.db(&mut txn, &format!("{}_{}", name, "consumer"))?;

        let file_num = consumer_db.get(&txn, &KEY_CONSUMER_FILE)?.unwrap();
        let bytes_read = consumer_db.get(&txn, &KEY_CONSUMER_BYTES_READ)?.unwrap();
        txn.commit()?;

        let mut reader = Reader::new(&env.root, name, file_num)?;
        if bytes_read > 0 {
            reader.set_bytes_read(bytes_read)?;
        }

        Ok(Consumer { env, producer_db, consumer_db, reader, chunks_to_keep: chunks_to_keep.unwrap_or(8) })
    }

    pub fn pop_front_n(&mut self, n: u64) -> Result<Vec<Item>, Box<dyn Error>> {
        let mut txn = self.env.write_txn()?;
        self.check_chunks_to_keep(&mut txn)?;

        let mut items = vec![];
        let mut delta = 0;
        for _ in 0..n {
            match self.reader.read() {
                Ok(item) => {
                    items.push(item);
                    delta += 1;
                },
                Err(_) => {
                    if self.rotate(&mut txn)? {
                        items.push(self.reader.read()?);
                        delta = 1;
                    } else {
                        break;
                    }
                }
            }
        }

        self.inc_offset(&mut txn, delta)?;
        txn.commit()?;
        Ok(items)
    }

    pub fn pop_front(&mut self) -> Result<Option<Item>, Box<dyn Error>> {
        let mut txn = self.env.write_txn()?;
        self.check_chunks_to_keep(&mut txn)?;

        match self.reader.read() {
            Ok(item) => {
                self.inc_offset(&mut txn, 1)?;
                txn.commit()?;
                return Ok(Some(item));
            },
            Err(_) => {
                if self.rotate(&mut txn)? {
                    let item = self.reader.read()?;
                    self.inc_offset(&mut txn, 1)?;
                    txn.commit()?;
                    return Ok(Some(item));
                } else {
                    txn.commit()?;
                    return Ok(None);
                }
            }
        }
    }

    fn inc_offset(&mut self, txn: &mut RwTxn, delta: u64) -> Result<(), Box<dyn Error>> {
        let offset = self.consumer_db.get(&txn, &KEY_CONSUMER_OFFSET)?.unwrap();
        self.consumer_db.put(txn, &KEY_CONSUMER_OFFSET, &(offset + delta))?;

        self.consumer_db.put(txn, &KEY_CONSUMER_BYTES_READ, &self.reader.get_bytes_read())?;
        Ok(())
    }

    fn check_chunks_to_keep(&mut self, txn: &mut RwTxn) -> Result<(), Box<dyn Error>> {
        let head = self.consumer_db.get(&txn, &KEY_CONSUMER_FILE)?.unwrap();
        let (tail, _) = self.producer_db.iter(&txn)?.last().transpose()?.unwrap();
        let chunk_to_remove: i64 = tail as i64 + 1 - head as i64 - self.chunks_to_keep as i64;
        for _ in 0..chunk_to_remove {
            self.rotate(txn)?;
        }

        Ok(())
    }

    fn rotate(&mut self, txn: &mut RwTxn) -> Result<bool, Box<dyn Error>> {
        let head = self.consumer_db.get(&txn, &KEY_CONSUMER_FILE)?.unwrap();
        let (tail, _) = self.producer_db.iter(&txn)?.last().transpose()?.unwrap();
        if tail > head {
            self.reader.rotate()?;
            self.producer_db.delete(txn, &head)?;
            self.consumer_db.put(txn, &KEY_CONSUMER_FILE, &(head + 1))?;
            self.consumer_db.put(txn, &KEY_CONSUMER_OFFSET, &0)?;
            self.consumer_db.put(txn, &KEY_CONSUMER_BYTES_READ, &0)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}