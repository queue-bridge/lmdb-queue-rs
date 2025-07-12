use lmdb::{Cursor, Database, DatabaseFlags, Error, Transaction, WriteFlags};
use lmdb_sys::{MDB_LAST, MDB_FIRST};
use super::env::Env;

use super::reader::Reader;
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

        let writer = Writer::new(&env.root, name, Producer::get_tail(db, &txn)?)?;

        txn.commit()?;

        Ok(Producer { env, db, writer })
    }

    fn get_tail<TXN>(db: Database, txn: &TXN) -> Result<u64, Error>
    where TXN: Transaction
    {
        let cur = txn.open_ro_cursor(db)?;
        if let (Some(key), _) = cur.get(None, None, MDB_LAST)? {
            slice_to_u64(key)
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
        let reader = Reader::new(&env.root, name, Comsumer::get_head(db, &txn)?)?;

        Ok(Comsumer { env, db, reader })
    }

    fn get_head<TXN>(db: Database, txn: &TXN) -> Result<u64, Error>
    where TXN: Transaction
    {
        let cur = txn.open_ro_cursor(db)?;
        if let (Some(_), value) = cur.get(None, None, MDB_FIRST)? {
            slice_to_u64(value)
        } else {
            Err(Error::NotFound)
        }
    }
}