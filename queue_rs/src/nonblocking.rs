use crate::mpmc;
use crate::mpmc::StartPosition;
use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use std::thread;
use std::time::Duration;

#[derive(Clone)]
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

#[derive(Clone)]
pub enum MpmcOperation {
    Add(Vec<Vec<u8>>),
    Next(usize, String, StartPosition),
    Length,
    DiskSize,
    GetLabels,
    RemoveLabel(String),
    Stop,
}

pub enum MpmcResponseVariant {
    Add(Result<()>),
    Next(Result<(Vec<Vec<u8>>, bool)>),
    Length(usize),
    Size(Result<usize>),
    GetLabels(Vec<String>),
    RemoveLabel(Result<bool>),
    Stop,
}

pub struct TypedResponse<T>(Receiver<T>);
pub type Response = TypedResponse<ResponseVariant>;
pub type MpmcResponse = TypedResponse<MpmcResponseVariant>;

impl<T> TypedResponse<T> {
    pub fn is_ready(&self) -> bool {
        !self.0.is_empty()
    }

    pub fn try_get(&self) -> Result<Option<T>> {
        let res = self.0.try_recv();
        if let Err(crossbeam_channel::TryRecvError::Empty) = &res {
            return Ok(None);
        }
        Ok(Some(res?))
    }

    pub fn get(&self) -> Result<T> {
        Ok(self.0.recv()?)
    }
}

type WorkingThread = Option<thread::JoinHandle<Result<()>>>;
type QueueSender<O, R> = Sender<(O, Sender<R>)>;
type QueueType<O, R> = (WorkingThread, QueueSender<O, R>);
pub struct NonBlockingQueueWrapper<O, R>(QueueType<O, R>, O)
where
    O: Clone + Send + Sync + 'static,
    R: Send + 'static;
pub type PersistentQueueWithCapacity = NonBlockingQueueWrapper<Operation, ResponseVariant>;
pub type MpmcQueue = NonBlockingQueueWrapper<MpmcOperation, MpmcResponseVariant>;

fn start_op_loop<O, R, F>(mut f: F, max_inflight_ops: usize) -> (WorkingThread, QueueSender<O, R>)
where
    F: FnMut(Receiver<(O, Sender<R>)>) -> Result<()> + Send + 'static,
    O: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    let (tx, rx) = crossbeam_channel::bounded::<(O, Sender<R>)>(max_inflight_ops);
    let handle = thread::spawn(move || f(rx));

    (Some(handle), tx)
}

impl<O, R> NonBlockingQueueWrapper<O, R>
where
    O: Clone + Send + Sync,
    R: Send,
{
    pub fn is_healthy(&self) -> bool {
        !self.0 .0.as_ref().map(|t| t.is_finished()).unwrap_or(true)
    }

    pub fn inflight_ops(&self) -> Result<usize> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        Ok(self.0 .1.len())
    }

    fn shutdown(&mut self) -> Result<()> {
        if self.is_healthy() {
            let (tx, rx) = crossbeam_channel::bounded(1);
            self.0 .1.send((self.1.clone(), tx))?;
            rx.recv()?;
            let thread_opt = self.0 .0.take();
            if let Some(thread) = thread_opt {
                thread.join().expect("Failed to join thread.")?;
            }
        }
        Ok(())
    }
}

impl<O, R> Drop for NonBlockingQueueWrapper<O, R>
where
    O: Clone + Send + Sync,
    R: Send,
{
    fn drop(&mut self) {
        self.shutdown().unwrap();
    }
}

impl PersistentQueueWithCapacity {
    pub fn new(
        path: &str,
        max_elements: usize,
        max_inflight_ops: usize,
        db_options: rocksdb::Options,
    ) -> Result<Self> {
        let mut queue =
            crate::PersistentQueueWithCapacity::new(path, max_elements, db_options).unwrap();
        let f = move |rx: Receiver<(Operation, Sender<ResponseVariant>)>| {
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
        };
        let (handle, tx) = start_op_loop(f, max_inflight_ops);
        Ok(Self((handle, tx), Operation::Stop))
    }

    pub fn len(&self) -> Result<Response> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((Operation::Length, tx))?;
        Ok(TypedResponse(rx))
    }

    pub fn disk_size(&self) -> Result<Response> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((Operation::DiskSize, tx))?;
        Ok(TypedResponse(rx))
    }

    pub fn payload_size(&self) -> Result<Response> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((Operation::PayloadSize, tx))?;
        Ok(TypedResponse(rx))
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
        Ok(TypedResponse(rx))
    }

    pub fn pop(&self, max_elements: usize) -> Result<Response> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((Operation::Pop(max_elements), tx))?;
        Ok(TypedResponse(rx))
    }
}

impl MpmcQueue {
    pub fn new(path: &str, ttl: Duration, max_inflight_ops: usize) -> Result<Self> {
        let mut queue = mpmc::MpmcQueue::new(path, ttl)?;
        let f = move |rx: Receiver<(MpmcOperation, Sender<MpmcResponseVariant>)>| {
            loop {
                match rx.recv() {
                    Ok((MpmcOperation::Add(values), resp_tx)) => {
                        let value_slices = values.iter().map(|e| e.as_slice()).collect::<Vec<_>>();
                        let resp = queue.add(&value_slices);
                        resp_tx.send(MpmcResponseVariant::Add(resp))?;
                    }
                    Ok((MpmcOperation::Next(max_elements, label, start_position), resp_tx)) => {
                        let resp = queue.next(max_elements, label.as_str(), start_position);
                        resp_tx.send(MpmcResponseVariant::Next(resp))?;
                    }
                    Ok((MpmcOperation::Length, resp_tx)) => {
                        let resp = queue.len();
                        resp_tx.send(MpmcResponseVariant::Length(resp))?;
                    }
                    Ok((MpmcOperation::DiskSize, resp_tx)) => {
                        let resp = queue.disk_size();
                        resp_tx.send(MpmcResponseVariant::Size(resp))?;
                    }
                    Ok((MpmcOperation::GetLabels, resp_tx)) => {
                        let resp = queue.get_labels();
                        resp_tx.send(MpmcResponseVariant::GetLabels(resp))?;
                    }
                    Ok((MpmcOperation::RemoveLabel(label), resp_tx)) => {
                        let resp = queue.remove_label(label.as_str());
                        resp_tx.send(MpmcResponseVariant::RemoveLabel(resp))?;
                    }
                    Ok((MpmcOperation::Stop, resp_tx)) => {
                        resp_tx.send(MpmcResponseVariant::Stop)?;
                        break;
                    }
                    Err(e) => return Err(anyhow::anyhow!("Error receiving operation: {}", e)),
                }
            }
            Ok(())
        };
        let (handle, tx) = start_op_loop(f, max_inflight_ops);
        Ok(Self((handle, tx), MpmcOperation::Stop))
    }

    pub fn disk_size(&self) -> Result<MpmcResponse> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((MpmcOperation::DiskSize, tx))?;
        Ok(TypedResponse(rx))
    }

    pub fn len(&self) -> Result<MpmcResponse> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((MpmcOperation::Length, tx))?;
        Ok(TypedResponse(rx))
    }

    pub fn add(&self, values: &[&[u8]]) -> Result<MpmcResponse> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((
            MpmcOperation::Add(values.iter().map(|e| e.to_vec()).collect()),
            tx,
        ))?;
        Ok(TypedResponse(rx))
    }

    pub fn next(
        &self,
        max_elts: usize,
        label: &str,
        start_position: StartPosition,
    ) -> Result<MpmcResponse> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((
            MpmcOperation::Next(max_elts, label.to_string(), start_position),
            tx,
        ))?;
        Ok(TypedResponse(rx))
    }

    pub fn get_labels(&self) -> Result<MpmcResponse> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0 .1.send((MpmcOperation::GetLabels, tx))?;
        Ok(TypedResponse(rx))
    }

    pub fn remove_label(&self, label: &str) -> Result<MpmcResponse> {
        if !self.is_healthy() {
            return Err(anyhow::anyhow!(
                "Queue is unhealthy: cannot use it anymore."
            ));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
             .1
            .send((MpmcOperation::RemoveLabel(label.to_string()), tx))?;
        Ok(TypedResponse(rx))
    }
}

#[cfg(test)]
mod tests {
    use crate::mpmc;
    use crate::mpmc::StartPosition;
    use std::time::Duration;

    #[test]
    fn persistent_queue_fresh_healthy() {
        let path = "/tmp/test_fresh_healthy".to_string();
        _ = crate::PersistentQueueWithCapacity::remove_db(&path);
        let queue =
            super::PersistentQueueWithCapacity::new(&path, 3, 1000, rocksdb::Options::default())
                .unwrap();
        assert!(queue.is_healthy());
        let resp = queue.len().unwrap().get().unwrap();
        assert!(matches!(resp, super::ResponseVariant::Length(0)));
        _ = crate::PersistentQueueWithCapacity::remove_db(&path);
    }

    #[test]
    fn persistent_queue_push_pop() {
        let path = "/tmp/test_push_pop".to_string();
        _ = crate::PersistentQueueWithCapacity::remove_db(&path);
        let queue =
            super::PersistentQueueWithCapacity::new(&path, 3, 1000, rocksdb::Options::default())
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
    fn persistent_queue_size() {
        let path = "/tmp/test_size".to_string();
        _ = crate::PersistentQueueWithCapacity::remove_db(&path);
        let queue =
            super::PersistentQueueWithCapacity::new(&path, 3, 1000, rocksdb::Options::default())
                .unwrap();
        let size_query = queue.disk_size().unwrap();
        let size = size_query.get().unwrap();
        assert!(matches!(size, super::ResponseVariant::Size(Ok(r)) if r > 0));
    }

    #[test]
    fn mpmc_queue_fresh_healthy() {
        let path = "/tmp/test_mpmc_fresh_healthy".to_string();
        _ = mpmc::MpmcQueue::remove_db(&path);
        let queue = super::MpmcQueue::new(&path, Duration::from_secs(10), 1000).unwrap();
        assert!(queue.is_healthy());
        let resp = queue.len().unwrap().get().unwrap();
        assert!(matches!(resp, super::MpmcResponseVariant::Length(0)));
        _ = mpmc::MpmcQueue::remove_db(&path);
    }

    #[test]
    fn mpmc_queue_add_next() {
        let path = "/tmp/test_mpmc_add_next".to_string();
        _ = mpmc::MpmcQueue::remove_db(&path);
        let queue = super::MpmcQueue::new(&path, Duration::from_secs(1), 1000).unwrap();
        assert!(queue.is_healthy());

        let resp = queue.len().unwrap().get().unwrap();
        assert!(matches!(resp, super::MpmcResponseVariant::Length(0)));

        let resp = queue.add(&[&[1u8, 2u8, 3u8]]).unwrap().get().unwrap();
        assert!(matches!(resp, super::MpmcResponseVariant::Add(Ok(()))));

        let resp = queue.len().unwrap().get().unwrap();
        assert!(matches!(resp, super::MpmcResponseVariant::Length(1)));
        let resp = queue
            .next(1, "label", StartPosition::Oldest)
            .unwrap()
            .get()
            .unwrap();
        assert!(
            matches!(resp, super::MpmcResponseVariant::Next(Ok(v)) if v == (vec![vec![1u8, 2u8, 3u8]], false))
        );
        let resp = queue.len().unwrap().get().unwrap();
        assert!(matches!(resp, super::MpmcResponseVariant::Length(1)));
        _ = mpmc::MpmcQueue::remove_db(&path);
    }

    #[test]
    fn mpmc_queue_size() {
        let path = "/tmp/test_mpmc_size".to_string();
        _ = mpmc::MpmcQueue::remove_db(&path);
        let queue = super::MpmcQueue::new(&path, Duration::from_secs(1), 1000).unwrap();
        let size_query = queue.disk_size().unwrap();
        let size = size_query.get().unwrap();
        assert!(matches!(size, super::MpmcResponseVariant::Size(Ok(r)) if r > 0));
        _ = mpmc::MpmcQueue::remove_db(&path);
    }
}
