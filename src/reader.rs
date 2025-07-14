use anyhow::{Result, anyhow};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    time::{SystemTime, UNIX_EPOCH}
};

pub struct Reader {
    fd: File,
    prefix: String,
    file_num: u64,
    bytes_read: u64,
}

pub struct Item {
    pub ts: u64,
    pub data: Vec<u8>,
}

impl Reader {
    pub fn new(root: &str, topic_name: &str, file_num: u64) -> Result<Self> {
        let prefix = format!("{}-{}", root, topic_name);
        let path = format!("{}-{:016x}", prefix, file_num);
        let fd = OpenOptions::new()
            .read(true)
            .open(path)?;

        Ok(Self { fd, prefix, file_num, bytes_read: 0 })
    }

    pub fn rotate(&mut self) -> Result<()> {
        let old_path = format!("{}-{:016x}", self.prefix, self.file_num);
        std::fs::remove_file(old_path)?;

        self.file_num = self.file_num + 1;
        self.bytes_read = 0;
        let path = format!("{}-{:016x}", self.prefix, self.file_num);
        self.fd = OpenOptions::new()
            .read(true)
            .open(path.clone())?;

        Ok(())
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
        self.bytes_read += data_len as u64 + 12;
        Ok(Item { ts: ts, data })
    }

    pub fn get_bytes_read(&self) -> u64 {
        self.bytes_read
    }

    pub fn set_bytes_read(&mut self, bytes_read: u64) -> Result<()> {
        self.fd.seek(SeekFrom::Start(bytes_read))?;
        self.bytes_read = bytes_read;
        Ok(())
    }
}

#[test]
fn test_reader() -> Result<()> {
    let mut reader = Reader::new("/tmp/foo", "bar", 0)?;

    let mut total = 0;

    loop {
        match reader.read() {
            Ok(item) => {
                total = total + 1;
                if total % (1024 * 1024) == 0 {
                    println!("Read {} messages, ts: {}, data: {}.", total, item.ts, String::from_utf8(item.data)?);
                }
            }
            Err(_) => {
                println!("Read {} messages.", total);
                if let Err(_) = reader.rotate() {
                    break;
                }
            }
        }
    }

    Ok(())
}