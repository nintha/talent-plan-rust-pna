//! struct or enum

use serde::{Deserialize, Serialize};
use std::net::TcpStream;
use std::io::Read;
use crate::Result;

#[allow(missing_docs)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Behavior {
    /// The user invokes kvs set mykey myvalue
    Set { key: String, value: String },
    /// The user invokes kvs get mykey
    Get { key: String },
    /// The user invokes kvs rm mykey
    Remove { key: String },
}

/// A message definition like redis protocol
#[derive(Debug)]
pub enum Msg {
    /// start with "+", end with "\r\n"
    Line(String),
    /// start with "-", end with "\r\n"
    Error(String),
    /// start with ":", end with "\r\n"
    Integer(i64),
    /// Bulk Strings are encoded in the following way:
    ///
    /// - A "$" byte followed by the number of bytes composing the string (a prefixed length), terminated by CRLF.
    /// - The actual string data.
    /// - A final CRLF.
    Bulk(Option<String>),
    /// RESP Arrays are sent using the following format:
    /// - A * character as the first byte, followed by the number of elements in the array as a decimal number, followed by CRLF.
    /// - An additional RESP type for every element of the Array.
    Array(Vec<Msg>),
}

impl Msg {
    /// convert `Vec<String>` to Bulk String Array
    pub fn build_bulk_array(strings: &Vec<String>) -> Self {
        let mut list = vec![];
        for s in strings {
            list.push(Msg::Bulk(Some(s.to_owned())));
        }

        Msg::Array(list)
    }

    /// convert Bulk String Array to `Vec<String>`, ignore Null Bulk String
    /// return `None` if not Bulk String Array
    pub fn try_to_vec_string(&self) -> Option<Vec<String>> {
        if let Msg::Array(array) = self {
            let mut list = vec![];
            for item in array {
                if let Msg::Bulk(bulk) = item {
                    if let Some(s) = bulk{
                        list.push(s.to_owned());
                    }
                }else{
                    // it is not Bulk String Array
                    return None;
                }
            }
            return Some(list);
        }
        None
    }

    /// convert it to `Vec<u8>`
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Msg::Line(line) => format!("+{}\r\n", line).as_bytes().to_vec(),
            Msg::Error(e) => format!("-{}\r\n", e).as_bytes().to_vec(),
            Msg::Integer(int) => format!(":{}\r\n", int).as_bytes().to_vec(),
            Msg::Bulk(text) => {
                if let Some(t) = text {
                    format!("${}\r\n{}\r\n", t.len(), t).as_bytes().to_vec()
                } else {
                    "$-1\r\n".as_bytes().to_vec()
                }
            }
            Msg::Array(array) => {
                let mut list = format!("*{}\r\n", array.len()).as_bytes().to_vec();
                for item in array {
                    list.append(&mut item.to_bytes());
                }
                list
            }
        }
    }
}

/// Support Msg Struct
pub trait MsgExtend {
    /// blocking read some bytes from `TcpStream`
    ///
    fn read_exact_return(&mut self, bytes_num: u32) -> Result<Vec<u8>>;

    /// blocking read until `\r\n`
    ///
    /// the return vec includes `\r\n` at the end
    fn read_until_crlf(&mut self) -> Result<Vec<u8>>;

    /// blocking read a `Msg` object
    fn read_msg(&mut self) -> Result<Msg>;
}

impl MsgExtend for TcpStream {
    /// blocking read some bytes from `TcpStream`
    ///
    fn read_exact_return(&mut self, bytes_num: u32) -> Result<Vec<u8>> {
        let mut data = vec![0u8; bytes_num as usize];
        self.read_exact(&mut data)?;
        return Ok(data);
    }

    /// blocking read until `\r\n`
    ///
    /// the return vec not includes `\r\n` at the end
    fn read_until_crlf(&mut self) -> Result<Vec<u8>> {
        let mut list = Vec::new();
        let mut cr_flag = false;
        let mut one_byte = [0u8; 1];
        loop {
            self.read_exact(&mut one_byte)?;
            list.push(one_byte[0]);
            if cr_flag && one_byte[0] == b'\n' {
                break;
            }
            cr_flag = one_byte[0] == b'\r';
        }
        list.pop(); // remove '\n'
        list.pop(); // remove '\r'
        Ok(list)
    }

    fn read_msg(&mut self) -> Result<Msg> {
        let cmd_type = self.read_exact_return(1)?;
        let msg = match cmd_type[0] {
            // Simple String
            b'+' => {
                let list = self.read_until_crlf()?;
                Msg::Line(String::from_utf8(list)?)
            }
            // Integer
            b':' => {
                let list = self.read_until_crlf()?;
                Msg::Integer(String::from_utf8(list)?.parse()?)
            }
            // Error
            b'-' => {
                let list = self.read_until_crlf()?;
                Msg::Error(String::from_utf8(list)?)
            }
            // Bulk String
            b'$' => {
                let head = self.read_until_crlf()?;
                let len: i64 = String::from_utf8(head)?.parse()?;
                if len < 0 {
                    Msg::Bulk(None)
                } else {
                    let content_bytes = self.read_exact_return(len as u32)?;
                    let content = String::from_utf8(content_bytes)?;
                    self.read_until_crlf()?; // read remain crlf
                    Msg::Bulk(Some(content))
                }
            }
            // Array
            b'*' => {
                let head = self.read_until_crlf()?;
                let len: u32 = String::from_utf8(head)?.parse()?;

                let mut list = vec![];
                for _ in 0..len {
                    list.push(self.read_msg()?);
                }

                Msg::Array(list)
            }
            _ => unreachable!(),
        };

        Ok(msg)
    }
}




