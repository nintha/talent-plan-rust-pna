//! struct or enum

use serde::{Deserialize, Serialize};

#[allow(missing_docs)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Behavior {
    /// The user invokes kvs set mykey myvalue
    Set { key: String, value: String },
    /// The user invokes kvs get mykey
    Get { key: String },
    /// The user invokes kvs rm mykey
    Remove { key: String },
}
