use lmdb::{Cursor, Database, DatabaseFlags, Error, RwTransaction, Transaction, WriteFlags};
use lmdb_sys::MDB_LAST;
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

pub struct Producer<'env> {
    env: &'env Env,
    db: Database,
    writer: Writer,
}

impl<'env> Producer<'env> {
    pub fn new(env: &'env Env, name: &str) -> Result<Self, anyhow::Error> {
        let mut txn = env.transaction_rw()?;
        let db = unsafe { txn.create_db(Some(name), DatabaseFlags::empty())? };

        let zero = &u64_to_bytes(0);
        if let Ok(_) = txn.put(db, &KEY_COMSUMER_FILE, zero, WriteFlags::NO_OVERWRITE) {
            txn.put(db, &KEY_COMSUMER_OFFSET, zero, WriteFlags::NO_OVERWRITE)?;
            txn.put(db, zero, zero, WriteFlags::NO_OVERWRITE)?;
        }

        let (tail_file, _) = Producer::get_tail(db, &txn)?;
        let writer = Writer::new(&env.root, name, tail_file)?;

        txn.commit()?;

        Ok(Producer { env, db, writer })
    }

    pub fn push_back_batch<'a, B>(&mut self, messages: &'a B) -> Result<(), anyhow::Error>
    where B: AsRef<[&'a [u8]]>
    {
        let mut txn = self.env.transaction_rw()?;
        let (tail_file, count) = Producer::get_tail(self.db, &txn)?;
        let size = self.writer.put_batch(messages)?;
        txn.put(self.db, &u64_to_bytes(tail_file), &u64_to_bytes(count + messages.as_ref().len() as u64), WriteFlags::empty())?;
        if size > 64 * 1024 * 1024 {
            self.writer.rotate()?;
            txn.put(self.db, &u64_to_bytes(tail_file + 1), &u64_to_bytes(0), WriteFlags::empty())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn push_back<'a>(&mut self, message: &'a [u8]) -> Result<(), anyhow::Error> {
        self.push_back_batch(&[message])
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
}

pub struct Comsumer<'env> {
    env: &'env Env,
    db: Database,
    reader: Reader,
}

impl <'env> Comsumer<'env> {
    pub fn new(env: &'env Env, name: &str) -> Result<Self, anyhow::Error> {
        let db = env.lmdb_env.open_db(Some(name))?;
        let txn = env.transaction_ro()?;
        let mut reader = Reader::new(&env.root, name, Comsumer::get_value(db, &txn, &KEY_COMSUMER_FILE)?)?;

        let offset = Comsumer::get_value(db, &txn, &KEY_COMSUMER_OFFSET)?;
        for _ in 0..offset {
            reader.read()?;
        }

        Ok(Comsumer { env, db, reader })
    }

    pub fn pop_front_n(&mut self, n: u64) -> Result<Vec<Item>, anyhow::Error> {
        let mut txn: RwTransaction<'_> = self.env.transaction_rw()?;
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
                        txn.commit()?;
                        return Ok(items);
                    }
                }
            }
        }

        self.bump_offset(&mut txn, delta)?;
        txn.commit()?;
        Ok(items)
    }

    pub fn pop_front(&mut self) -> Result<Option<Item>, anyhow::Error> {
        let mut txn = self.env.transaction_rw()?;
        match self.reader.read() {
            Ok(item) => {
                self.bump_offset(&mut txn, 1)?;
                txn.commit()?;
                return Ok(Some(item));
            },
            Err(_) => {
                if self.rotate(&mut txn)? {
                    let item = self.reader.read()?;
                    self.bump_offset(&mut txn, 1)?;
                    txn.commit()?;
                    return Ok(Some(item));
                } else {
                    txn.commit()?;
                    return Ok(None);
                }
            }
        }
    }

    fn rotate(&mut self, txn: &mut RwTransaction) -> Result<bool, anyhow::Error> {
        let head = Comsumer::get_value(self.db, txn, &KEY_COMSUMER_FILE)?;
        let (tail, _) = Producer::get_tail(self.db, txn)?;
        if tail > head {
            self.reader.rotate()?;
            txn.del(self.db, &u64_to_bytes(head), None)?;
            txn.put(self.db, &KEY_COMSUMER_FILE, &u64_to_bytes(head + 1), WriteFlags::empty())?;
            txn.put(self.db, &KEY_COMSUMER_OFFSET, &u64_to_bytes(0), WriteFlags::empty())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn bump_offset(&self, txn: &mut RwTransaction, delta: u64) -> Result<(), Error> {
        let old_offset = Comsumer::get_value(self.db, txn, &KEY_COMSUMER_OFFSET)?;
        txn.put(self.db, &KEY_COMSUMER_OFFSET, &u64_to_bytes(old_offset + delta), WriteFlags::empty())?;
        Ok(())
    }

    fn get_value<TXN>(db: Database, txn: &TXN, key: &[u8]) -> Result<u64, Error>
    where TXN: Transaction
    {
        let value = txn.get(db, &key)?;
        slice_to_u64(value)
    }    
}