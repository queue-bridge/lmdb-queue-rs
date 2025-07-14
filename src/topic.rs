use lmdb::{Cursor, Database, DatabaseFlags, Error, RwTransaction, Transaction, WriteFlags};
use lmdb_sys::{MDB_LAST, MDB_SET_RANGE};
use super::env::Env;

use super::reader::{Reader, Item};
use super::writer::Writer;

pub static KEY_COMSUMER_FILE: [u8; 1] = [0];
pub static KEY_COMSUMER_OFFSET: [u8; 2] = [0, 0];

pub fn slice_to_u64(slice: &[u8]) -> Result<u64, Error> {
    let arr: [u8; 8] = slice.try_into().map_err(|_| Error::Corrupted)?;
    Ok(u64::from_be_bytes(arr))
}

pub fn u64_to_bytes(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

pub trait Topic {
    fn get_env(&self) -> &Env;
    fn get_db(&self) -> Database;

    fn lag(&self) -> Result<u64, Error> {
        let txn = self.get_env().transaction_ro()?;
        let db = self.get_db();
        let head = Self::get_value(db, &txn, &KEY_COMSUMER_FILE)?;
        let (tail, _) = Self::get_tail(db, &txn)?;
        let total = (head..tail + 1)
            .map(|v| Self::get_value(db, &txn, &u64_to_bytes(v)).unwrap_or(0))
            .reduce(|acc, v| acc + v)
            .unwrap_or(0);

        let head_offset = Self::get_value(db, &txn, &KEY_COMSUMER_OFFSET)?;
        Ok(total - head_offset)
    }

    fn inc(&self, txn: &mut RwTransaction, key: &[u8], delta: u64) -> Result<(), Error> {
        let mut cur = txn.open_rw_cursor(self.get_db())?;
        let (_, old) = cur.get(Some(&key), None, MDB_SET_RANGE)?;
        let old_val = slice_to_u64(old)?;
        cur.put(&key, &u64_to_bytes(old_val + delta), WriteFlags::CURRENT)
    }

    fn replace(&self, txn: &mut RwTransaction, key: &[u8], value: u64) -> Result<(), Error> {
        let mut cur = txn.open_rw_cursor(self.get_db())?;
        cur.get(Some(&key), None, MDB_SET_RANGE)?;
        cur.put(&key, &u64_to_bytes(value), WriteFlags::CURRENT)
    }

    fn get_tail<TXN>(db: Database, txn: &TXN) -> Result<(u64, u64), Error>
    where TXN: Transaction
    {
        let cur = txn.open_ro_cursor(db)?;
        if let (Some(key), value) = cur.get(None, None, MDB_LAST)? {
            Ok((slice_to_u64(key)?, slice_to_u64(value)?))
        } else {
            Err(Error::NotFound)
        }
    }

    fn get_value<TXN>(db: Database, txn: &TXN, key: &[u8]) -> Result<u64, Error>
    where TXN: Transaction
    {
        let value = txn.get(db, &key)?;
        slice_to_u64(value)
    }
}

pub struct Producer<'env> {
    env: &'env Env,
    db: Database,
    writer: Writer,
    chunk_size: u64,
}

impl<'env> Topic for Producer<'env> {
    fn get_env(&self) -> &Env {
        self.env
    }

    fn get_db(&self) -> Database {
        self.db
    }
}

impl<'env> Producer<'env> {
    pub fn new(env: &'env Env, name: &str, chunk_size: Option<u64>) -> Result<Self, anyhow::Error> {
        let mut txn = env.transaction_rw()?;
        let db = unsafe { txn.create_db(Some(name), DatabaseFlags::empty())? };

        let zero = &u64_to_bytes(0);
        if let Ok(_) = txn.put(db, &KEY_COMSUMER_FILE, zero, WriteFlags::NO_OVERWRITE) {
            txn.put(db, &KEY_COMSUMER_OFFSET, zero, WriteFlags::NO_OVERWRITE)?;
            txn.put(db, zero, zero, WriteFlags::NO_OVERWRITE)?;
        }

        let (tail_file, _) = Self::get_tail(db, &txn)?;
        let writer = Writer::new(&env.root, name, tail_file)?;

        txn.commit()?;

        Ok(Producer { env, db, writer, chunk_size: chunk_size.unwrap_or(64 * 1024 * 1024) })
    }

    pub fn push_back_batch<'a, B>(&mut self, messages: &'a B) -> Result<(), anyhow::Error>
    where B: AsRef<[&'a [u8]]>
    {
        let mut txn = self.env.transaction_rw()?;
        let (mut tail_file, _) = Self::get_tail(self.db, &txn)?;
        if self.writer.file_size()? > self.chunk_size {
            self.writer.rotate()?;
            tail_file += 1;
            txn.put(self.db, &u64_to_bytes(tail_file), &u64_to_bytes(0), WriteFlags::empty())?;
        }
        self.writer.put_batch(messages)?;
        self.inc(&mut txn, &u64_to_bytes(tail_file), messages.as_ref().len() as u64)?;
        txn.commit()?;
        Ok(())
    }

    pub fn push_back<'a>(&mut self, message: &'a [u8]) -> Result<(), anyhow::Error> {
        self.push_back_batch(&[message])
    }
}

pub struct Comsumer<'env> {
    env: &'env Env,
    db: Database,
    reader: Reader,
    chunks_to_keep: u64,
}

impl <'env> Topic for Comsumer<'env> {
    fn get_env(&self) -> &Env {
        self.env
    }

    fn get_db(&self) -> Database {
        self.db
    }    
}

impl <'env> Comsumer<'env> {
    pub fn new(env: &'env Env, name: &str, chunks_to_keep: Option<u64>) -> Result<Self, anyhow::Error> {
        let db = env.lmdb_env.open_db(Some(name))?;
        let txn = env.transaction_ro()?;
        let mut reader = Reader::new(&env.root, name, Self::get_value(db, &txn, &KEY_COMSUMER_FILE)?)?;

        let offset = Self::get_value(db, &txn, &KEY_COMSUMER_OFFSET)?;
        for _ in 0..offset {
            reader.read()?;
        }

        Ok(Comsumer { env, db, reader, chunks_to_keep: chunks_to_keep.unwrap_or(8) })
    }

    pub fn pop_front_n(&mut self, n: u64) -> Result<Vec<Item>, anyhow::Error> {
        let mut txn: RwTransaction<'_> = self.env.transaction_rw()?;
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

        self.inc(&mut txn, &KEY_COMSUMER_OFFSET, delta)?;
        txn.commit()?;
        Ok(items)
    }

    pub fn pop_front(&mut self) -> Result<Option<Item>, anyhow::Error> {
        let mut txn = self.env.transaction_rw()?;
        self.check_chunks_to_keep(&mut txn)?;

        match self.reader.read() {
            Ok(item) => {
                self.inc(&mut txn, &KEY_COMSUMER_OFFSET, 1)?;
                txn.commit()?;
                return Ok(Some(item));
            },
            Err(_) => {
                if self.rotate(&mut txn)? {
                    let item = self.reader.read()?;
                    self.inc(&mut txn, &KEY_COMSUMER_OFFSET, 1)?;
                    txn.commit()?;
                    return Ok(Some(item));
                } else {
                    txn.commit()?;
                    return Ok(None);
                }
            }
        }
    }

    fn check_chunks_to_keep(&mut self, txn: &mut RwTransaction) -> Result<(), anyhow::Error> {
        let head = Self::get_value(self.db, txn, &KEY_COMSUMER_FILE)?;
        let (tail, _) = Self::get_tail(self.db, txn)?;
        let chunk_to_remove: i64 = tail as i64 + 1 - head as i64 - self.chunks_to_keep as i64;
        for _ in 0..chunk_to_remove {
            self.rotate(txn)?;
        }

        Ok(())
    }

    fn rotate(&mut self, txn: &mut RwTransaction) -> Result<bool, anyhow::Error> {
        let head = Self::get_value(self.db, txn, &KEY_COMSUMER_FILE)?;
        let (tail, _) = Self::get_tail(self.db, txn)?;
        if tail > head {
            self.reader.rotate()?;
            txn.del(self.db, &u64_to_bytes(head), None)?;
            self.replace(txn, &KEY_COMSUMER_FILE, head + 1)?;
            self.replace(txn, &KEY_COMSUMER_OFFSET, 0)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}