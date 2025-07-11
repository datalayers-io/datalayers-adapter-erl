use rustler::{Decoder, Error, MapIterator, NifResult, Term};

#[derive(Clone, Debug)]
/// The connect Options for the client connecting to the Datalayers server via Arrow Flight SQL protocol.
pub struct ClientOpts {
    /// The hostname of the Datalayers database server.
    pub host: Option<String>,
    /// The port number on which the Datalayers database server is listening.
    pub port: Option<u16>,
    /// The username for authentication when connecting to the database.
    pub username: Option<String>,
    /// The password for authentication when connecting to the database.
    pub password: Option<String>,
    /// The optional TLS certificate for secure connections.
    /// The certificate is self-signed by Datalayers and is used as the pem file by the client to certify itself.
    pub tls_cert: Option<String>,
}

impl Default for ClientOpts {
    fn default() -> ClientOpts {
        ClientOpts {
            host: Some("127.0.0.1".to_string()),
            port: Some(8360),
            username: Some("admin".to_string()),
            password: Some("public".to_string()),
            tls_cert: None,
        }
    }
}

impl<'a> Decoder<'a> for ClientOpts {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let mut opts = Self::default();
        for (key, value) in MapIterator::new(term).ok_or(Error::BadArg)? {
            match key.atom_to_string()?.as_ref() {
                "host" => opts.host = Some(value.decode()?),
                "port" => opts.port = Some(value.decode()?),
                "username" => opts.username = Some(value.decode()?),
                "password" => opts.password = Some(value.decode()?),
                "tls_cert" => opts.tls_cert = Some(value.decode()?),
                _ => (),
            }
        }
        Ok(opts)
    }
}

impl ClientOpts {
    pub fn protocol(&self) -> &str {
        if self.tls_cert.is_some() {
            "https"
        } else {
            "http"
        }
    }

    pub fn format_uri(&self) -> String {
        format!(
            "{}://{}:{}",
            self.protocol(),
            self.host.as_deref().unwrap_or_default(),
            self.port.unwrap_or_default()
        )
    }
}
