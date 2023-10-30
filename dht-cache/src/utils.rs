use std::time::{SystemTime, UNIX_EPOCH};
use local_ip_address::list_afinet_netifas;
use std::net::IpAddr;

pub fn get_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

pub fn get_addresses_of_local_interfaces() -> Vec<String> {
    let network_interfaces = list_afinet_netifas().unwrap();

    let mut to_ret = Vec::new();

    for (_name, ip) in network_interfaces.iter() {
        if ip.to_string() != "127.0.0.1" && matches!(ip, IpAddr::V4(_)) {
             to_ret.push(ip.to_string());
        }
    }

    to_ret
}

pub fn is_local_peer(multiaddr: &str) -> bool {
    let my_addresses = get_addresses_of_local_interfaces();
    log::info!("My addresses {:?}", my_addresses);
    for ip in my_addresses {
        if multiaddr.to_string().contains(&ip) {
            return true;
        }
    }
    false
}