use anyhow::Result;
use crossbeam_channel::Sender;
use std::thread;

pub enum Operation {
    Push(Vec<Vec<u8>>),
    Pop(usize),
    Length,
    Size,
    Stop,
}

pub enum Response {
    Push(Result<()>),
    Pop(Result<Vec<Vec<u8>>>),
    Length(usize),
    Size(Result<usize>),
    Stop,
}

pub struct PersistentQueueWithCapacity(
    (
        Option<thread::JoinHandle<Result<()>>>,
        Sender<(Operation, Sender<Response>)>,
    ),
);

fn start_op_loop(
    path: &str,
    max_elements: usize,
    max_inflight_ops: usize,
    db_options: rocksdb::Options,
) -> (
    Option<thread::JoinHandle<Result<()>>>,
    Sender<(Operation, Sender<Response>)>,
) {
    let mut queue =
        crate::PersistentQueueWithCapacity::new(&path, max_elements, db_options).unwrap();
    let (tx, rx) = crossbeam_channel::bounded::<(Operation, Sender<Response>)>(max_inflight_ops);
    let handle = thread::spawn(move || {
        loop {
            match rx.recv() {
                Ok((Operation::Push(values), resp_tx)) => {
                    let value_slices = values.iter().map(|e| e.as_slice()).collect::<Vec<_>>();
                    let resp = queue.push(&value_slices);
                    resp_tx.send(Response::Push(resp))?;
                }
                Ok((Operation::Pop(max_elts), resp_tx)) => {
                    let resp = queue.pop(max_elts);
                    resp_tx.send(Response::Pop(resp))?;
                }
                Ok((Operation::Length, resp_tx)) => {
                    let resp = queue.len();
                    resp_tx.send(Response::Length(resp))?;
                }
                Ok((Operation::Size, resp_tx)) => {
                    let resp = queue.size();
                    resp_tx.send(Response::Size(resp))?;
                }
                Ok((Operation::Stop, resp_tx)) => {
                    resp_tx.send(Response::Stop)?;
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

    pub fn shutdown(&mut self) -> Result<()> {
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

    pub fn len(&self) -> Result<usize> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((Operation::Length, tx))?;
        match rx.recv()? {
            Response::Length(len) => Ok(len),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub fn size(&self) -> Result<usize> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((Operation::Size, tx))?;
        match rx.recv()? {
            Response::Size(size) => size,
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub fn push(&mut self, values: &[&[u8]]) -> Result<()> {
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
        match rx.recv()? {
            Response::Push(resp) => resp,
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub fn pop(&mut self, max_elements: usize) -> Result<Vec<Vec<u8>>> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((Operation::Pop(max_elements), tx))?;
        match rx.recv()? {
            Response::Pop(resp) => resp,
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
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
        assert_eq!(queue.len().unwrap(), 0);
        _ = crate::PersistentQueueWithCapacity::remove_db(&path);
    }
}
