use bytes;
use bytes::BufMut;

use tokio_io::codec;

use std::io;
use rand::Rng;
use msg_framing::*;

use utils;

#[derive(Debug)]
#[derive(Clone)]
pub enum RelayMessage {
	ShareMessage {
            data: String,
        },
        AuthMessage {
            password: String,
	},
}
	
pub struct RelayMsgFramer {
    flag_auth: u16,
    flag_data: u16,
}

impl RelayMsgFramer {
    pub fn new() -> RelayMsgFramer {
        RelayMsgFramer {
            flag_auth: 0xef01,
            flag_data: 0xfe01,
        }
    }
}

impl codec::Encoder for RelayMsgFramer {
    type Item = RelayMessage;
    type Error = io::Error;

    fn encode(&mut self, msg: RelayMessage, res: &mut bytes::BytesMut) -> Result<(), io::Error> {
        match msg {
            RelayMessage::ShareMessage { ref data } => {
                res.reserve(10 + data.len());
                res.put_u16_le(self.flag_data);
                res.put_u32_le(10 + data.len() as u32);
                let mut rng = rand::thread_rng();
                res.put_u32_le(rng.gen::<u32>());
                res.put_slice(data.as_bytes());
            },
            RelayMessage::AuthMessage { ref password } => {
                res.reserve(10 + password.len());
                res.put_u16_le(self.flag_auth);
                res.put_u32_le(10 + password.len() as u32);
                let mut rng = rand::thread_rng();
                res.put_u32_le(rng.gen::<u32>());
                res.put_slice(password.as_bytes());
            },
        }
        Ok(())
    }
}

impl codec::Decoder for RelayMsgFramer {
    type Item = RelayMessage;
    type Error = io::Error;

    fn decode(&mut self, bytes: &mut bytes::BytesMut) -> Result<Option<RelayMessage>, io::Error> {
        if bytes.len() < 10 { return Ok(None) }
        let len = utils::slice_to_le32(&bytes[2..6]) as usize;
        if bytes.len() < len { return Ok(None) }

        let flag = (bytes[0] as u16) << 8 | bytes[1] as u16;
        match flag {
            0xfe01 => {
                let password = match String::from_utf8(bytes[10..len].to_vec()) {
                    Ok(string) => string,
                    Err(_) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, CodecError));
                    },
                };
                let msg = RelayMessage::AuthMessage {
                    password,
                };
                bytes.advance(len);
                Ok(Some(msg))
            },
            0xef01 => {
                let share = match String::from_utf8(bytes[10..len].to_vec()) {
                    Ok(string) => string,
                    Err(_) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, CodecError));
                    },
                };
                let msg = RelayMessage::ShareMessage {
                    data: share,
                };
                bytes.advance(len);
                Ok(Some(msg))
            },
            _ => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, CodecError));
            }
        }
    }
}


