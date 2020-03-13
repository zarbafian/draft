use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use log::{debug, info};

type MessageHandler = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    New(MessageHandler),
    Terminate,
}
struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let thread = thread::Builder::new()
            .name(format!("message handler {}", id))
            .spawn(move ||{
                info!("Worker started");
                loop {
                    let handler = receiver.lock().unwrap().recv().unwrap();
                    match handler {
                        Message::New(handler) => {
                            debug!("Received a message");
                            handler()
                        },
                        Message::Terminate => {
                            debug!("Will terminate");
                            break
                        },
                    }
                }
                info!("Worker terminated");
            }).unwrap();

        Worker{
            id,
            thread: Some(thread),
        }
    }
}

pub struct ThreadPool {
    sender: mpsc::Sender<Message>,
    workers: Vec<Worker>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool{
        assert!(size > 0, "Invalid size for thread pool: {}", size);

        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 1..=size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }
        ThreadPool{
            sender,
            workers,
        }
    }

    pub fn execute(&self, handler: MessageHandler) {
        self.sender.send(Message::New(handler)).unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        info!("Will send terminate message to all workers");

        for _ in &self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        info!("Shutting down all workers");

        for worker in &mut self.workers {
            info!("Shutting down worker {}", worker.id);
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}