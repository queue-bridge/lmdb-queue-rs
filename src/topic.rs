use lmdb::{Cursor, Database, DatabaseFlags, Error, Transaction, WriteFlags};
use lmdb_sys::{mdb_set_compare, MDB_val, MDB_LAST};
use libc::{c_int, memcmp};
use super::env::Env;
use std::ptr;

pub static KEY_PRODUCER: &'static [u8] = b"producer_head";

pub fn slice_to_u64(slice: &[u8]) -> Result<u64, Error> {
    let arr: [u8; 8] = slice.try_into().map_err(|_| Error::Corrupted)?;
    Ok(u64::from_ne_bytes(arr))
}

pub fn u64_to_bytes(v: u64) -> [u8; 8] {
    v.to_ne_bytes()
}

pub struct Topic<'env> {
    env: &'env Env,
    db: Database,
}

pub extern "C" fn mdb_int_cmp_u64(a: *const MDB_val, b: *const MDB_val) -> c_int {
    unsafe {
        let a_val = ptr::read_unaligned((*a).mv_data as *const u64);
        let b_val = ptr::read_unaligned((*b).mv_data as *const u64);
        a_val.cmp(&b_val) as c_int  // reverse for DESC
    }
}

extern "C" fn desc_cmp(a: *const MDB_val, b: *const MDB_val) -> c_int {
    unsafe {
        let a_size = (*a).mv_size;
        let b_size = (*b).mv_size;

        // DESC order by size
        if a_size > b_size {
            return -1;
        }

        if a_size < b_size {
            return 1;
        }

        match a_size {
            s if s == size_of::<u64>() => mdb_int_cmp_u64(a, b),
            _ => memcmp((*a).mv_data, (*b).mv_data, a_size)
        }
    }
}

impl<'env> Topic<'env> {
    pub fn new(env: &'env Env, name: &str) -> Result<Topic<'env>, Error>  {
        let mut txn = env.transaction_rw()?;
        let db = unsafe { txn.create_db(Some(name), DatabaseFlags::empty())? };
        unsafe {
            mdb_set_compare(txn.txn(), db.dbi(), desc_cmp as *mut _);
        }

        let zero = &u64_to_bytes(0);
        if let Ok(_) = txn.put(db, &KEY_PRODUCER, zero, WriteFlags::NO_OVERWRITE) {
            txn.put(db, zero, zero, WriteFlags::NO_OVERWRITE)?;
        }

        txn.commit()?;

        Ok(Topic { env, db })
    }

    pub fn get_producer_head_file(&self) -> Result<u64, Error> {
        let txn = self.env.transaction_ro()?;
        let cur = txn.open_ro_cursor(self.db)?;
        if let (Some(key), _) = cur.get(None, None, MDB_LAST)? {
            slice_to_u64(key)
        } else {
            Err(Error::NotFound)
        }
    }

    pub fn get_producer_head(&self) -> Result<u64, Error> {
        self.get_producer_head_with_txn(&self.env.transaction_ro()?)
    }


    pub fn get_producer_head_with_txn<TXN>(&self, txn: &TXN) -> Result<u64, Error>
    where
        TXN: Transaction
    {
        slice_to_u64(txn.get(self.db, &KEY_PRODUCER)?)
    }
}