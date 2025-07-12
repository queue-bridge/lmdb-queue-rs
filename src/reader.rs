use anyhow::{Result, anyhow};
use std::{fs::{File, OpenOptions}, io::Read, time::{SystemTime, UNIX_EPOCH}};

struct Reader {
    fd: File,
}

pub struct Item {
    pub ts: u64,
    pub data: Vec<u8>,
}

impl Reader {
    pub fn new(root: &str, topic_name: &str, file_num: u64) -> Result<Self> {
        let path = format!("{}-{}-{:016x}", root, topic_name, file_num);
        let fd = OpenOptions::new()
            .read(true)
            .open(path)?;

        Ok(Self { fd })
    }

    pub fn read(&mut self) -> Result<Item> {
        let mut head = vec![0; 4 + 8];
        self.fd.read_exact(&mut head)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock went backwards")
            .as_secs();

        let data_len = u32::from_ne_bytes(head[0..4].try_into()?);
        let ts = u64::from_ne_bytes(head[4..12].try_into()?);

        if ts > now || ts < now - 86400 * 10 {
            return Err(anyhow!("Message expired."));
        }

        let mut data = vec![0; data_len as usize];
        self.fd.read_exact(&mut data)?;
        Ok(Item { ts: ts, data })
    }
}

#[test]
fn test_reader() -> Result<()> {
    let mut reader = Reader::new("/tmp/foo", "test", 0)?;

    let mut total = 0;
    loop {
        if let Ok(item) = reader.read() {
            total = total + 1;
            if total % (1024 * 1024) == 0 {
                println!("Read {} messages, ts: {}, data: {}.", total, item.ts, String::from_utf8(item.data)?);
            }
        } else {
            break;
        }
    }

    println!("Read {} messages.", total);
    Ok(())
}