// With this mod, shares and weak blocks will be sent to kafka message queue,
// with information in json format with attributes of type `ShareMessage`.
use connection_maintainer::*;
use relay_msg_framing::*;
use std::sync::{Arc, RwLock};
use tokio;
use futures::future;

use futures::sync::mpsc;
use bitcoin::blockdata::block::BlockHeader;
use bitcoin::util::hash::Sha256dHash;
use std::io;

use std::net::ToSocketAddrs;
use futures::{Stream, Sink};

use serde_json;

use utils;

struct RelayState {
    is_connected: bool,
    stream: Option<mpsc::UnboundedSender<RelayMessage>>,
}

struct RelayHandler {
    password: String,
    state: RwLock<RelayState>,
}

impl RelayHandler {
    pub fn new(password: String) -> Arc<RelayHandler> {
        Arc::new(RelayHandler {
            password,
            state: RwLock::new(RelayState{
                is_connected: false,
                stream: None,
            }),
        })
    }

    pub fn send(&self, msg: RelayMessage) -> bool {
        let mut us = self.state.write().unwrap();
        if us.is_connected && !us.stream.is_none() {
            match us.stream {
                Some(ref mut stream) => {
                    let _ = stream.start_send(msg.clone());
                    println!("Sent {:?}", msg);
                    return true;
                },
                None => {},
            }
        }
        false
    }

}

impl ConnectionHandler<RelayMessage> for Arc<RelayHandler> {
    type Stream = mpsc::UnboundedReceiver<RelayMessage>;
    type Framer = RelayMsgFramer;

    fn new_connection(&self) -> (RelayMsgFramer, mpsc::UnboundedReceiver<RelayMessage>) {
        let (mut tx, rx) = mpsc::unbounded();
        let mut us = self.state.write().unwrap();
        match tx.start_send(RelayMessage::AuthMessage {
            password: self.password.clone(),
        }) {
            Ok(_) => {
                us.stream = Some(tx);
                us.is_connected = true;
            },
            Err(_) => {
                us.is_connected = false;
                us.stream = None;
                println!("Receiver disconnected before login");
            }
        }

        (RelayMsgFramer::new(), rx)
    }

    fn connection_closed(&self) {
        let mut us = self.state.write().unwrap();
        us.is_connected = false;
        us.stream = None;
    }

    fn handle_message(&self, msg: RelayMessage) -> Result<(), io::Error> {
        println!("Received message from receiver {:?}", msg);
        Ok(())
    }
}


#[derive(Clone)]
pub struct RelaySubmitterSettings {
        receiver_hosts: Vec<String>,
        password: String,
}

#[derive(Clone)]
pub struct RelaySubmitterState {
        settings: RelaySubmitterSettings,
        handlers: Vec<Arc<RelayHandler>>,
        tx: mpsc::UnboundedSender<RelayMessage>,
}

impl RelaySubmitterState {
    pub fn send(&mut self, msg: RelayMessage) {
        let mut sent = false;
        for handler in self.handlers.iter() {
            if handler.send(msg.clone()) {
                sent = true;
                break;
            }
        }
        if !sent {
            println!("Can not send message out currently!");
            self.send_message(msg);
        }
    }

    pub fn send_message(&self, msg: RelayMessage) {
        let mut sender = self.tx.clone();
        let _ = sender.start_send(msg);
    }

    pub fn send_share(&self, data: String) {
        self.send_message(RelayMessage::ShareMessage {
            data,
        });
    }
}


pub fn init_submitter_settings() -> RelaySubmitterSettings {
	RelaySubmitterSettings {
                receiver_hosts: Vec::new(),
                password: String::new(),
	}
}

pub fn print_submitter_parameters() {
	println!("--receiver_address - the receiver address, multiple is allowed");
	println!("--receiver_password - the receiver password for authentication");
}

/// Returns true if the given parameter could be parsed into a setting this submitter understands
pub fn parse_submitter_parameter(settings: &mut RelaySubmitterSettings, arg: &str) -> bool {
        if arg.starts_with("--receiver_address") {
                match arg.split_at(19).1.to_socket_addrs() {
                        Err(_) => {
                                println!("Bad address resolution: {}", arg);
                                panic!();
                        },
                        Ok(_) => settings.receiver_hosts.push(arg.split_at(19).1.to_string())
                }
                true
        } else if arg.starts_with("--receiver_password") {
                settings.password = arg.split_at(20).1.to_string();
                true
	} else {
		false
	}
}

pub fn setup_submitter(settings: RelaySubmitterSettings) -> RelaySubmitterState {
        if settings.receiver_hosts.is_empty() {
		println!("Need at least some receiver address");
		panic!();
	}

        let mut handlers = Vec::new();
        for addr in settings.receiver_hosts.iter() {
            let handler = RelayHandler::new(settings.password.clone());
            handlers.push(handler.clone());
            ConnectionMaintainer::new(addr.to_string(), handler).make_connection();
        }

        let (tx, rx) = mpsc::unbounded();
			
        let state = RelaySubmitterState {
                settings,
                handlers,
                tx,
        };

        let mut sender = state.clone();
        tokio::spawn(rx.for_each(move |msg| {
            sender.send(msg);
            future::result(Ok(()))
        }));
        state
}

// Serialize pool share
#[derive(Serialize)]
struct ShareMessage {
	user: String,       // miner username
	worker: String,     // miner workername
        height: i64,        // block height
        prev_block_hash: String, // prev block hash
	payout: u64,        // claimed value of the share - payout will be min(median share value, this value)
	client_target: u8,  // client target
	leading_zeros: u8,  // share target
	version: u32,       // version
	nbits: u32,         // nbits
	time: u32,          // share tsp
	is_good_block: bool,// potential good block tag
	is_weak_block: bool,// weak block tag
}

// Serialize pool weak block
#[derive(Serialize)]
struct WeakBlockMessage {
	user: String,       // miner username
	worker: String,     // miner workername
        height: i64,        // block height
        prev_block_hash: String, // prev block hash
	payout: u64,        // claimed value of the share - payout will be min(median share value, this value)
	client_target: u8,  // client target
	leading_zeros: u8,  // share target
	version: u32,       // version
	nbits: u32,         // nbits
	time: u32,          // share tsp
	hash: String,       // weak block header hash
	is_good_block: bool,// potential good block tag
	is_weak_block: bool,// weak block tag
}

pub fn share_submitted(state: &RelaySubmitterState, user_id: &Vec<u8>, user_tag_1: &Vec<u8>, value: u64, header: &BlockHeader, leading_zeros: u8, required_leading_zeros: u8, height: i64) {
	println!("Got valid share with value {} from \"{}\" from machine identified as \"{}\"", value, String::from_utf8_lossy(user_id), String::from_utf8_lossy(user_tag_1));
        state.send_share(serde_json::to_string(&ShareMessage {
				user: String::from_utf8_lossy(&user_id).to_string(),
				worker: String::from_utf8_lossy(&user_tag_1).to_string(),
				payout: value,
				client_target: required_leading_zeros,
				leading_zeros,
				version: header.version,
				nbits: header.bits,
				time: header.time,
                                height,
                                prev_block_hash: utils::bytes_to_hex(&header.prev_blockhash[..]),
				is_good_block: false,
				is_weak_block: false,
			}).unwrap());
}

#[allow(dead_code)]
pub fn event_submitted(_state: &RelaySubmitterState, event: &String) {
    println!("New Event: {}", event);
}

pub fn weak_block_submitted(state: &RelaySubmitterState, user_id: &Vec<u8>, user_tag_1: &Vec<u8>, value: u64, header: &BlockHeader, txn: &Vec<Vec<u8>>, _extra_block_data: &Vec<u8>,
	leading_zeros: u8, required_leading_zeros: u8, block_hash: &Sha256dHash, height: i64) {
	println!("Got valid weak block with value {} from \"{}\" with {} txn from machine identified as \"{}\"", value, String::from_utf8_lossy(user_id), txn.len(), String::from_utf8_lossy(user_tag_1));
	let hash = &block_hash[..];
	let (block_target, negative, overflow) = utils::nbits_to_target(header.bits);
	if negative || overflow {
		println!("We got invalid block target: negative or overflow!");
		return;
	}
	let is_good_block = utils::does_hash_meet_target(hash, &block_target[..]);
        state.send_share(serde_json::to_string(&WeakBlockMessage {
				user: String::from_utf8_lossy(&user_id).to_string(),
				worker: String::from_utf8_lossy(&user_tag_1).to_string(),
				payout: value,
				client_target: required_leading_zeros,
				leading_zeros,
				version: header.version,
				nbits: header.bits,
				time: header.time,
                                height,
                                prev_block_hash: utils::bytes_to_hex(&header.prev_blockhash[..]),
				hash: utils::bytes_to_hex(hash),
				is_good_block,
				is_weak_block: true,
			}).unwrap());
}

