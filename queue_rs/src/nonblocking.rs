use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use std::thread;

pub enum Operation {
    Push(Vec<Vec<u8>>),
    Pop(usize),
    Length,
    PayloadSize,
    DiskSize,
    Stop,
}

pub enum ResponseVariant {
    Push(Result<()>),
    Pop(Result<Vec<Vec<u8>>>),
    Length(usize),
    Size(Result<usize>),
    Stop,
}

pub struct Response(Receiver<ResponseVariant>);

impl Response {
    pub fn is_ready(&self) -> bool {
        !self.0.is_empty()
    }

    pub fn try_get(&self) -> Result<Option<ResponseVariant>> {
        let res = self.0.try_recv();
        if let Err(crossbeam_channel::TryRecvError::Empty) = &res {
            return Ok(None);
        }
        Ok(Some(res?))
    }

    pub fn get(&self) -> Result<ResponseVariant> {
        Ok(self.0.recv()?)
    }
}

type WorkingThread = Option<thread::JoinHandle<Result<()>>>;
type QueueSender = Sender<(Operation, Sender<ResponseVariant>)>;
type QueueType = (WorkingThread, QueueSender);

pub struct PersistentQueueWithCapacity(QueueType);

fn start_op_loop(
    path: &str,
    max_elements: usize,
    max_inflight_ops: usize,
    db_options: rocksdb::Options,
) -> (WorkingThread, QueueSender) {
    let mut queue =
        crate::PersistentQueueWithCapacity::new(path, max_elements, db_options).unwrap();
    let (tx, rx) =
        crossbeam_channel::bounded::<(Operation, Sender<ResponseVariant>)>(max_inflight_ops);
    let handle = thread::spawn(move || {
        loop {
            match rx.recv() {
                Ok((Operation::Push(values), resp_tx)) => {
                    let value_slices = values.iter().map(|e| e.as_slice()).collect::<Vec<_>>();
                    let resp = queue.push(&value_slices);
                    resp_tx.send(ResponseVariant::Push(resp))?;
                }
                Ok((Operation::Pop(max_elements), resp_tx)) => {
                    let resp = queue.pop(max_elements);
                    resp_tx.send(ResponseVariant::Pop(resp))?;
                }
                Ok((Operation::Length, resp_tx)) => {
                    let resp = queue.len();
                    resp_tx.send(ResponseVariant::Length(resp))?;
                }
                Ok((Operation::DiskSize, resp_tx)) => {
                    let resp = queue.disk_size();
                    resp_tx.send(ResponseVariant::Size(resp))?;
                }
                Ok((Operation::PayloadSize, resp_tx)) => {
                    let resp = queue.payload_size();
                    resp_tx.send(ResponseVariant::Size(Ok(resp as usize)))?;
                }
                Ok((Operation::Stop, resp_tx)) => {
                    resp_tx.send(ResponseVariant::Stop)?;
                    break;
                }
                Err(e) => return Err(anyhow::anyhow!("Error receiving operation: {}", e)),
            }
        }
        Ok(())
    });

    (Some(handle), tx)
}

impl PersistentQueueWithCapacity {
    pub fn new(
        path: &str,
        max_elements: usize,
        max_inflight_ops: usize,
        db_options: rocksdb::Options,
    ) -> Result<Self> {
        let (handle, tx) = start_op_loop(path, max_elements, max_inflight_ops, db_options);
        Ok(Self((handle, tx)))
    }

    pub fn is_healthy(&self) -> bool {
        !self.0 .0.as_ref().map(|t| t.is_finished()).unwrap_or(true)
    }

    fn shutdown(&mut self) -> Result<()> {
        if self.is_healthy() {
            let (tx, rx) = crossbeam_channel::bounded(1);
            self.0 .1.send((Operation::Stop, tx))?;
            rx.recv()?;
            let thread_opt = self.0 .0.take();
            if let Some(thread) = thread_opt {
                thread.join().expect("Failed to join thread.")?;
            }
        }
        Ok(())
    }

    pub fn len(&self) -> Result<Response> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((Operation::Length, tx))?;
        Ok(Response(rx))
    }

    pub fn inflight_ops(&self) -> Result<usize> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        Ok(self.0 .1.len())
    }

    pub fn disk_size(&self) -> Result<Response> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((Operation::DiskSize, tx))?;
        Ok(Response(rx))
    }

    pub fn payload_size(&self) -> Result<Response> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((Operation::PayloadSize, tx))?;
        Ok(Response(rx))
    }

    pub fn push(&self, values: &[&[u8]]) -> Result<Response> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((
            Operation::Push(values.iter().map(|e| e.to_vec()).collect()),
            tx,
        ))?;
        Ok(Response(rx))
    }

    pub fn pop(&self, max_elements: usize) -> Result<Response> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((Operation::Pop(max_elements), tx))?;
        Ok(Response(rx))
    }
}

impl Drop for PersistentQueueWithCapacity {
    fn drop(&mut self) {
        self.shutdown().unwrap();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn fresh_healthy() {
        let path = "/tmp/test_fresh_healthy".to_string();
        _ = crate::PersistentQueueWithCapacity::remove_db(&path);
        let queue =
            super::PersistentQueueWithCapacity::new(&path, 1000, 1000, rocksdb::Options::default())
                .unwrap();
        assert!(queue.is_healthy());
        let resp = queue.len().unwrap().get().unwrap();
        assert!(matches!(resp, super::ResponseVariant::Length(0)));
        _ = crate::PersistentQueueWithCapacity::remove_db(&path);
    }

    #[test]
    fn push_pop() {
        let path = "/tmp/test_push_pop".to_string();
        _ = crate::PersistentQueueWithCapacity::remove_db(&path);
        let queue =
            super::PersistentQueueWithCapacity::new(&path, 1000, 1000, rocksdb::Options::default())
                .unwrap();
        assert!(queue.is_healthy());

        let resp = queue.len().unwrap().get().unwrap();
        assert!(matches!(resp, super::ResponseVariant::Length(0)));

        let resp = queue.push(&[&[1u8, 2u8, 3u8]]).unwrap().get().unwrap();
        assert!(matches!(resp, super::ResponseVariant::Push(Ok(()))));

        let resp = queue.payload_size().unwrap().get().unwrap();
        assert!(matches!(resp, super::ResponseVariant::Size(Ok(3))));

        let resp = queue.len().unwrap().get().unwrap();
        assert!(matches!(resp, super::ResponseVariant::Length(1)));
        let resp = queue.pop(1).unwrap().get().unwrap();
        assert!(
            matches!(resp, super::ResponseVariant::Pop(Ok(v)) if v == vec![vec![1u8, 2u8, 3u8]])
        );
        let resp = queue.len().unwrap().get().unwrap();
        assert!(matches!(resp, super::ResponseVariant::Length(0)));
        _ = crate::PersistentQueueWithCapacity::remove_db(&path);
    }

    #[test]
    fn size() {
        let path = "/tmp/test_size".to_string();
        _ = crate::PersistentQueueWithCapacity::remove_db(&path);
        let queue =
            super::PersistentQueueWithCapacity::new(&path, 1000, 1000, rocksdb::Options::default())
                .unwrap();
        let size_query = queue.disk_size().unwrap();
        let size = size_query.get().unwrap();
        assert!(matches!(size, super::ResponseVariant::Size(Ok(r)) if r > 0));
    }
}
