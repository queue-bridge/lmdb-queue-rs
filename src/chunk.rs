use lmdb::{Cursor, Database, DatabaseFlags, Error, Transaction, WriteFlags, RwTransaction, RoTransaction};
use lmdb_sys::{mdb_set_compare, MDB_val, MDB_LAST};
use libc::{c_int, memcmp, size_t};
use super::env::Env;
use super::topic::{mdb_int_cmp_u64, slice_to_u64, u64_to_bytes};

pub struct Chunk {
    env: Env,
    db: Database,
}

impl Chunk {
    pub fn new(root: &str, topic_name: &str, file_num: u64, map_size: Option<size_t>) -> Result<Self, Error> {
        let path = format!("{}_{}_{:05}", root, topic_name, file_num);
        let env = Env::new(path, Some(1), map_size)?;
        let txn = env.transaction_rw()?;
        let db = unsafe { txn.create_db(None, DatabaseFlags::empty())? };
        unsafe {
            mdb_set_compare(txn.txn(), db.dbi(), mdb_int_cmp_u64 as *mut _);
        }

        drop(txn);
        Ok(Chunk { env, db })
    }

    pub fn transaction_rw(&self) -> Result<RwTransaction, Error> {
        self.env.transaction_rw()
    }

    pub fn transaction_ro(&self) -> Result<RoTransaction, Error> {
        self.env.transaction_ro()
    }

    pub fn put(&self, txn: &mut RwTransaction, id: u64, value: &[u8]) -> Result<(), Error> {
        txn.put(self.db, &u64_to_bytes(id), &value, WriteFlags::empty())?;
        Ok(())
    }
}