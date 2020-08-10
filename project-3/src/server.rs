//! kvs server

use crate::engines::KvsEngine;
use crate::error::KvsError;
use crate::model::{Behavior, MsgExtend, Msg};
use crate::Result;
use std::net::{TcpListener, TcpStream};
use std::io::Write;

#[allow(missing_docs)]
pub struct KvsServer {
    binding_address: String,
    engine: Box<dyn KvsEngine>,
}

impl KvsServer {
    /// create with address and engine
    pub fn new(binding_address: String, engine: Box<dyn KvsEngine>) -> Self {
        KvsServer { binding_address, engine }
    }

    /// bind tcp port and handle connection
    pub fn start(&mut self) -> Result<()> {
        let listener = TcpListener::bind(&self.binding_address)?;

        // accept connections and process them serially
        for stream in listener.incoming() {
            let stream = stream?;
            if let Err(e) = self.handle_client(stream.try_clone()?){
                log::error!("connection error, peer={}, {}", &stream.peer_addr()?,  e);
            }
        }
        Ok(())
    }

    fn handle_client(&mut self, mut stream: TcpStream) -> Result<()> {
        let msg_to_behavior: fn(msg: Msg) -> Result<Behavior> = |msg| {
            let arguments_option = msg.try_to_vec_string();
            if arguments_option.is_none() {
                Err(KvsError::InvalidArgumentNumber)?
            }
            let arguments = arguments_option.unwrap();
            match arguments[0].as_str() {
                "get" => {
                    if arguments.len() < 2 {
                        Err(KvsError::InvalidArgumentNumber)?
                    }
                    return Ok(Behavior::Get { key: arguments[1].to_owned() });
                }
                "set" => {
                    if arguments.len() < 3 {
                        Err(KvsError::InvalidArgumentNumber)?
                    }
                    return Ok(Behavior::Set {
                        key: arguments[1].to_owned(),
                        value: arguments[2].to_owned(),
                    });
                }
                "rm" => {
                    if arguments.len() < 2 {
                        Err(KvsError::InvalidArgumentNumber)?
                    }
                    return Ok(Behavior::Remove { key: arguments[1].to_owned() });
                }
                _ => {}
            }

            Err(KvsError::InvalidArgumentNumber)?
        };

        loop {
            let msg = stream.read_msg()?;
            let behavior = msg_to_behavior(msg)?;

            let resp_msg = match behavior {
                Behavior::Set { key, value } => {
                    match self.engine.set(key, value) {
                        Ok(_) => Msg::Bulk(None),
                        Err(e) => Msg::Error(e.to_string()),
                    }
                }
                Behavior::Get { key } => {
                    match self.engine.get(key) {
                        Ok(value) => Msg::Bulk(value),
                        Err(e) => Msg::Error(e.to_string()),
                    }
                }
                Behavior::Remove { key } => {
                    match self.engine.remove(key) {
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