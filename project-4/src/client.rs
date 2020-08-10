//! kvs client

use std::io::Write;
use std::net::TcpStream;

use crate::model::{Msg, MsgExtend};
use crate::Result;

#[allow(missing_docs)]
#[allow(dead_code)]
pub struct KvsClient {
    server_address: String,
    stream: TcpStream,
}

impl KvsClient {
    /// connect to server with address
    pub fn connect(address: String) -> Result<Self> {
        let stream = TcpStream::connect(&address)?;
        Ok(Self { stream, server_address: address })
    }

    /// send message to server, and wait for the response
    pub fn request_msg(&mut self, msg: Msg) -> Result<Msg>{
        self.stream.write_all(&msg.to_bytes())?;
        self.stream.read_msg()
    }
}
