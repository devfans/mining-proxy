extern crate base64;
extern crate bitcoin;
extern crate secp256k1;

use secp256k1::Secp256k1;

use secp256k1::rand::thread_rng;
use bitcoin::network::constants::Network;
use bitcoin::util::privkey;
use std::env;


fn help() {
        println!("USAGE: tool method params");
}

fn method_newwifprivatekey () {
    let secp = Secp256k1::new();
    let (sk, _pk) = secp.generate_keypair(&mut thread_rng());
    let private_key = privkey::Privkey {
        compressed: true,
        network: Network::Bitcoin,
        key: sk,
    };
    println!("new privateKey: {}", private_key);
}


fn main() {
        let args: Vec<String> = env::args().collect();
        if args.len() < 2 {
            println!("Invalid method name");
            help();
            return;
        }
        let method = &args[1];
        println!("Calling {}", method);
        if method == "newwifprivatekey" {
            method_newwifprivatekey();
        } else {
            println!("Invalid/Unknown method");
        }
}




