//! A classic fixed-size thread pool.
//!
//! It is the textbook demonstration of the shared-state primitives this
//! project is meant to show off: jobs are handed to workers over an `mpsc`
//! channel whose receiving end is shared between every worker thread via
//! `Arc<Mutex<Receiver>>`. Each worker loops, locks the mutex just long enough
//! to pull the next job, then runs it with no lock held.

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

/// A unit of work to run on a worker thread.
type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
    workers: Vec<JoinHandle<()>>,
    /// `Option` so `Drop` can close the channel (by dropping the sender) and
    /// thereby tell the workers to exit.
    sender: Option<mpsc::Sender<Job>>,
}

impl ThreadPool {
    /// Create a pool with `size` worker threads (clamped to at least 1).
    pub fn new(size: usize) -> ThreadPool {
        let size = size.max(1);
        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            let receiver = Arc::clone(&receiver);
            let handle = thread::Builder::new()
                .name(format!("evrika-worker-{}", id))
                .spawn(move || worker_loop(receiver))
                .expect("spawn worker");
            workers.push(handle);
        }

        ThreadPool {
            workers,
            sender: Some(sender),
        }
    }

    /// Submit a job to be run by some worker.
    pub fn execute<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Some(sender) = &self.sender {
            // If every worker has panicked the send may fail; ignore it — the
            // connection simply will not be served.
            let _ = sender.send(Box::new(job));
        }
    }
}

fn worker_loop(receiver: Arc<Mutex<mpsc::Receiver<Job>>>) {
    loop {
        // Lock only to receive; release before running so other workers can
        // grab the next job concurrently.
        let job = {
            let guard = receiver.lock().unwrap();
            guard.recv()
        };
        match job {
            Ok(job) => job(),
            // Sender dropped -> pool shutting down.
            Err(_) => break,
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // Dropping the sender closes the channel; each worker's `recv` then
        // returns `Err` and the loop exits.
        drop(self.sender.take());
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}
