use rustler::{Decoder, Error, MapIterator, NifResult, Term};
use std::time::Duration;

/// Per-stage flight timeout used when the connect options do not set one.
pub const DEFAULT_TIMEOUT_MS: u64 = 15_000;

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
    /// Per-stage flight timeout in milliseconds, decided by the caller.
    /// Defaults to [`DEFAULT_TIMEOUT_MS`]; `0` is treated as "not set".
    pub timeout_ms: Option<u64>,
}

impl Default for ClientOpts {
    fn default() -> ClientOpts {
        ClientOpts {
            host: Some("127.0.0.1".to_string()),
            port: Some(8360),
            username: Some("admin".to_string()),
            password: Some("public".to_string()),
            tls_cert: None,
            timeout_ms: None,
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
                "timeout" => opts.timeout_ms = Some(value.decode()?),
                _ => (),
            }
        }
        Ok(opts)
    }
}

impl ClientOpts {
    /// The per-stage flight timeout, as decided by whoever built these options.
    pub fn timeout(&self) -> Duration {
        match self.timeout_ms {
            Some(ms) if ms > 0 => Duration::from_millis(ms),
            _ => Duration::from_millis(DEFAULT_TIMEOUT_MS),
        }
    }

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
            self.host.clone().unwrap_or_default(),
            self.port.unwrap_or(8360)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientOpts, DEFAULT_TIMEOUT_MS};
    use std::time::Duration;

    #[test]
    fn timeout_defaults_to_15s_when_unset() {
        assert_eq!(
            ClientOpts::default().timeout(),
            Duration::from_millis(DEFAULT_TIMEOUT_MS)
        );
        assert_eq!(
            ClientOpts {
                timeout_ms: Some(0),
                ..Default::default()
            }
            .timeout(),
            Duration::from_millis(DEFAULT_TIMEOUT_MS)
        );
    }

    #[test]
    fn timeout_is_decided_by_the_caller() {
        let opts = ClientOpts {
            timeout_ms: Some(2_500),
            ..Default::default()
        };
        assert_eq!(opts.timeout(), Duration::from_millis(2_500));
    }
}
