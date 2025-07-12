use anyhow::Result;
use std::{fs::{File, OpenOptions}, io::Write, time::{SystemTime, UNIX_EPOCH}};

struct Producer {
    fd: File,
}

impl Producer {
    pub fn new(root: &str, topic_name: &str, file_num: u64) -> Self {
        let path = format!("{}-{}-{:016x}", root, topic_name, file_num);
        let fd = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        Self { fd }
    }

    fn append(&mut self, message: &[u8]) -> Result<()> {
        let mut buf = Vec::with_capacity(4 + 8 + message.len());
        let len = message.len() as u32;
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock went backwards")
            .as_secs() ;
        buf.extend_from_slice(&len.to_ne_bytes());
        buf.extend_from_slice(&ts.to_ne_bytes());
        buf.extend_from_slice(message);

        self.fd.write(&buf)?;
        Ok(())
    }

    pub fn put_batch<'a, B>(&mut self, messages: &'a B) -> Result<u64>
    where B: AsRef<[&'a [u8]]>
    {
        for message in messages.as_ref() {
            self.append(message)?;
        }
        Ok(self.fd.metadata()?.len())
    }

    pub fn put(&mut self, message: &[u8]) -> Result<u64> {
        self.append(message)?;
        Ok(self.fd.metadata()?.len())
    }
}

#[test]
fn test_put_batch() -> Result<()> {
    let mut producer = Producer::new("/tmp/foo", "test", 0);

    for i in 0..1024*256 {
        let messages: Vec<Vec<u8>> = (0..10)
            .map(|j| format!("{}_{}", i, j).into_bytes())
            .collect();

        let batch: Vec<&[u8]> = messages.iter().map(|v| v.as_slice()).collect();
        producer.put_batch(&batch)?;
    }

    Ok(())
}
