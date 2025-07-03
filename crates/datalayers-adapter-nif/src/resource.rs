use rustler::ResourceArc;
use std::sync::Mutex;

use crate::client::Client;

pub struct ClientResource(pub Mutex<Option<Client>>);

impl rustler::Resource for ClientResource {}

impl ClientResource {
    pub fn new(client: Client) -> ResourceArc<Self> {
        ResourceArc::new(Self(Mutex::new(Some(client))))
    }
}
