//! kvs server

use std::io::Write;
use std::net::{TcpListener, TcpStream};

use crate::engines::KvsEngine;
use crate::model::{Behavior, Msg, MsgExtend};
use crate::Result;
use crate::thread_pool::ThreadPool;

#[allow(missing_docs)]
pub struct KvsServer<KE: KvsEngine,TP: ThreadPool> {
    binding_address: String,
    engine: KE,
    thread_pool: TP,
}

impl<KE: KvsEngine, TP: ThreadPool> KvsServer<KE, TP> {
    /// create with address and engine
    pub fn new(binding_address: String, engine: KE, thread_pool: TP) -> Self {
        KvsServer { binding_address, engine ,thread_pool}
    }

    /// bind tcp port and handle connection
    pub fn start(&mut self) -> Result<()> {
        let listener = TcpListener::bind(&self.binding_address)?;

        // accept connections and process them serially
        for stream in listener.incoming() {
            let mut stream = stream?;
            let peer_addr = stream.peer_addr()?.clone();
            let engine_clone = self.engine.clone();
            self.thread_pool.spawn(move || {
                if let Err(e) = Self::handle_client(engine_clone, &mut stream) {
                    log::error!("connection error, peer={}, {}", peer_addr,  e);
                };
            });
        }
        Ok(())
    }

    fn handle_client(engine: KE, stream: &mut TcpStream) -> Result<()> {
        loop {
            let msg = stream.read_msg()?;
            let behavior = msg.try_to_behavior()?;

            let resp_msg = match behavior {
                Behavior::Set { key, value } => {
                    match engine.set(key, value) {
                        Ok(_) => Msg::Bulk(None),
                        Err(e) => Msg::Error(e.to_string()),
                    }
                }
                Behavior::Get { key } => {
                    match engine.get(key) {
                        Ok(value) => Msg::Bulk(value),
                        Err(e) => Msg::Error(e.to_string()),
                    }
                }
                Behavior::Remove { key } => {
                    match engine.remove(key) {
                        Ok(_) => Msg::Bulk(None),
                        Err(e) => Msg::Error(e.to_string()),
                    }
                }
            };
            stream.write_all(&resp_msg.to_bytes())?;
        }
        // Ok(())
    }
}