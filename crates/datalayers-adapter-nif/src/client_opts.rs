use rustler::{Decoder, Error, MapIterator, NifResult, Term};
use std::path::Path;
use std::time::Duration;

/// Per-stage flight timeout used when the connect options do not set one.
pub const DEFAULT_TIMEOUT_MS: u64 = 15_000;

/// Whole-request deadline used when the connect options do not set one.
///
/// A request may span several stages (`prepare` + `execute` + `do_get`, plus a
/// statement rebuild), so the per-stage timeout alone bounds it only loosely.
/// This bounds the total time a request can occupy (or wait for) a slot.
///
/// The caller does not have to provide it: the default matches the per-stage
/// default and stays well below EMQX's own `request_ttl` default (45s), so a
/// reply always reaches the buffer worker before it considers the request
/// expired (an expired request's reply is dropped as a late reply).
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 15_000;
/// Largest accepted whole-request deadline. Keep in sync with the Erlang header.
pub const MAX_REQUEST_TIMEOUT_MS: u64 = 86_400_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsVerify {
    Peer,
    None,
}

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
    /// Whether the caller explicitly requested TLS.
    ///
    /// When omitted, a CA path still implies TLS for compatibility with the original
    /// API. New callers should set this explicitly so a lost CA can not silently
    /// downgrade the connection to plaintext.
    pub tls_enable: Option<bool>,
    /// The CA bundle used to verify the Datalayers server certificate.
    pub tls_cacertfile: Option<String>,
    /// Server certificate verification mode. Only peer verification is supported.
    pub verify: Option<TlsVerify>,
    /// The name used for SNI and certificate hostname verification.
    pub tls_server_name: Option<String>,
    /// Per-stage flight timeout in milliseconds, decided by the caller.
    /// Defaults to [`DEFAULT_TIMEOUT_MS`]; `0` is treated as "not set".
    pub timeout_ms: Option<u64>,
    /// Whole-request deadline in milliseconds, decided by the caller.
    /// Defaults to [`DEFAULT_REQUEST_TIMEOUT_MS`]; `0` is treated as "not set".
    pub request_timeout_ms: Option<u64>,
    /// A TLS decoding error. Validation reports it before any network I/O.
    pub(crate) tls_config_error: Option<String>,
}

impl Default for ClientOpts {
    fn default() -> ClientOpts {
        ClientOpts {
            host: Some("127.0.0.1".to_string()),
            port: Some(8360),
            username: Some("admin".to_string()),
            password: Some("public".to_string()),
            tls_enable: None,
            tls_cacertfile: None,
            verify: None,
            tls_server_name: None,
            timeout_ms: None,
            request_timeout_ms: None,
            tls_config_error: None,
        }
    }
}

fn record_tls_error(opts: &mut ClientOpts, reason: impl Into<String>) {
    if opts.tls_config_error.is_none() {
        opts.tls_config_error = Some(reason.into());
    }
}

fn decode_tls_verify(term: Term<'_>) -> Result<TlsVerify, String> {
    let value = match term.atom_to_string() {
        Ok(atom) => atom,
        Err(_) => term
            .decode::<String>()
            .map_err(|_| "verify must be verify_peer".to_string())?,
    };
    match value.as_str() {
        "verify_peer" => Ok(TlsVerify::Peer),
        "verify_none" => Ok(TlsVerify::None),
        _ => Err("verify must be verify_peer; verify_none is not supported".to_string()),
    }
}

impl<'a> Decoder<'a> for ClientOpts {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let mut opts = Self::default();
        for (key, value) in MapIterator::new(term).ok_or(Error::BadArg)? {
            match key.atom_to_string()?.as_ref() {
                "host" => opts.host = Some(value.decode()?),
                "port" => {
                    let port = value.decode::<u32>()?;
                    opts.port = Some(u16::try_from(port).map_err(|_| Error::BadArg)?);
                }
                "username" => opts.username = Some(value.decode()?),
                "password" => opts.password = Some(value.decode()?),
                "tls_enable" => match value.decode::<bool>() {
                    Ok(enabled) => opts.tls_enable = Some(enabled),
                    Err(_) => record_tls_error(&mut opts, "tls_enable must be a boolean"),
                },
                "tls_cacertfile" => match value.decode::<String>() {
                    Ok(path) => opts.tls_cacertfile = Some(path),
                    Err(_) => record_tls_error(&mut opts, "tls_cacertfile must be a string"),
                },
                "tls_cert" => record_tls_error(
                    &mut opts,
                    "tls_cert is not supported; use tls_cacertfile for the CA bundle",
                ),
                "verify" => match decode_tls_verify(value) {
                    Ok(verify) => opts.verify = Some(verify),
                    Err(reason) => record_tls_error(&mut opts, reason),
                },
                "tls_server_name" => match value.decode::<String>() {
                    Ok(name) => opts.tls_server_name = Some(name),
                    Err(_) => record_tls_error(&mut opts, "tls_server_name must be a string"),
                },
                "certfile" | "keyfile" => record_tls_error(
                    &mut opts,
                    "TLS client certificates are not supported by Datalayers",
                ),
                // Both timeouts are decoded leniently: an unusable value (a
                // string like "15s", a float, a negative number) must fall back
                // to the default the way the Erlang side treats it, instead of
                // raising badarg in the caller -- which is the socket process.
                "timeout" => opts.timeout_ms = value.decode().ok(),
                "request_timeout" => opts.request_timeout_ms = value.decode().ok(),
                _ => (),
            }
        }
        Ok(opts)
    }
}

impl ClientOpts {
    /// Return the configured CA bundle path.
    pub fn tls_ca_path(&self) -> Option<&str> {
        self.tls_cacertfile.as_deref()
    }

    /// Whether the caller requested TLS.
    ///
    /// A legacy CA-only configuration still enables TLS. New callers should use
    /// `tls_enable` so an omitted CA fails validation instead of selecting HTTP.
    pub fn tls_enabled(&self) -> bool {
        self.tls_enable
            .unwrap_or_else(|| self.tls_ca_path().is_some())
    }

    /// The name used for SNI and server certificate hostname verification.
    pub fn tls_server_name(&self) -> String {
        self.tls_server_name
            .clone()
            .unwrap_or_else(|| self.host.clone().unwrap_or_default())
    }

    /// Validate the complete TLS option set before any file or network I/O.
    ///
    /// Returns `None` for validated plaintext mode and `Some(path)` for TLS mode.
    pub fn validate_tls(&self) -> Result<Option<&str>, String> {
        if let Some(reason) = &self.tls_config_error {
            return Err(reason.clone());
        }

        let ca_path = self.tls_ca_path();
        let has_tls_options =
            ca_path.is_some() || self.verify.is_some() || self.tls_server_name.is_some();

        if self.tls_enable == Some(false) && has_tls_options {
            return Err("TLS options require tls_enable = true".to_string());
        }

        if !self.tls_enabled() {
            if has_tls_options {
                return Err("TLS options require a CA bundle".to_string());
            }
            return Ok(None);
        }

        let ca_path = ca_path.ok_or_else(|| "tls_cacertfile is required for TLS".to_string())?;
        if ca_path.trim().is_empty() {
            return Err("tls_cacertfile must not be empty".to_string());
        }
        if !Path::new(ca_path).is_absolute() {
            return Err("tls_cacertfile must be an absolute path".to_string());
        }

        if self.verify == Some(TlsVerify::None) {
            return Err("verify_none is not supported; use verify_peer".to_string());
        }

        if self
            .tls_server_name
            .as_deref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err("tls_server_name must not be empty".to_string());
        }

        Ok(Some(ca_path))
    }

    /// The per-stage flight timeout, as decided by whoever built these options.
    pub fn timeout(&self) -> Duration {
        match self.timeout_ms {
            Some(ms) if ms > 0 => Duration::from_millis(ms),
            _ => Duration::from_millis(DEFAULT_TIMEOUT_MS),
        }
    }

    /// The whole-request deadline, as decided by whoever built these options.
    pub fn request_timeout(&self) -> Duration {
        match self.request_timeout_ms {
            Some(ms) if ms > 0 && ms <= MAX_REQUEST_TIMEOUT_MS => Duration::from_millis(ms),
            _ => Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
        }
    }

    pub fn protocol(&self) -> &str {
        if self.tls_enabled() { "https" } else { "http" }
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
    use super::{ClientOpts, DEFAULT_REQUEST_TIMEOUT_MS, DEFAULT_TIMEOUT_MS, TlsVerify};
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

    #[test]
    fn request_timeout_defaults_to_15s_when_unset() {
        assert_eq!(
            ClientOpts::default().request_timeout(),
            Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS)
        );
        assert_eq!(
            ClientOpts {
                request_timeout_ms: Some(0),
                ..Default::default()
            }
            .request_timeout(),
            Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS)
        );
    }

    #[test]
    fn request_timeout_is_decided_by_the_caller_and_independent_of_the_stage_timeout() {
        let opts = ClientOpts {
            timeout_ms: Some(2_500),
            request_timeout_ms: Some(700),
            ..Default::default()
        };
        assert_eq!(opts.timeout(), Duration::from_millis(2_500));
        assert_eq!(opts.request_timeout(), Duration::from_millis(700));
    }

    #[test]
    fn out_of_range_request_timeout_falls_back_to_the_default() {
        let opts = ClientOpts {
            request_timeout_ms: Some(u64::MAX),
            ..Default::default()
        };
        assert_eq!(
            opts.request_timeout(),
            Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS)
        );
    }

    fn tls_opts() -> ClientOpts {
        ClientOpts {
            tls_enable: Some(true),
            tls_cacertfile: Some("/test/ca.pem".to_string()),
            verify: Some(TlsVerify::Peer),
            ..Default::default()
        }
    }

    #[test]
    fn tls_is_plaintext_by_default() {
        let opts = ClientOpts::default();
        assert!(!opts.tls_enabled());
        assert_eq!(opts.protocol(), "http");
        assert_eq!(opts.validate_tls().unwrap(), None);

        let explicitly_disabled = ClientOpts {
            tls_enable: Some(false),
            ..Default::default()
        };
        assert_eq!(explicitly_disabled.validate_tls().unwrap(), None);
    }

    #[test]
    fn explicit_tls_requires_a_ca_bundle() {
        let opts = ClientOpts {
            tls_enable: Some(true),
            verify: Some(TlsVerify::Peer),
            ..Default::default()
        };
        let err = opts.validate_tls().unwrap_err();
        assert!(err.contains("tls_cacertfile is required"), "{err}");
    }

    #[test]
    fn explicit_ca_returns_the_validated_path() {
        assert_eq!(tls_opts().validate_tls().unwrap(), Some("/test/ca.pem"));
    }

    #[test]
    fn verify_none_is_rejected() {
        let opts = ClientOpts {
            verify: Some(TlsVerify::None),
            ..tls_opts()
        };
        let err = opts.validate_tls().unwrap_err();
        assert!(err.contains("verify_none is not supported"), "{err}");
    }

    #[test]
    fn tls_options_are_not_accepted_when_tls_is_disabled() {
        let opts = ClientOpts {
            tls_enable: Some(false),
            ..tls_opts()
        };
        let err = opts.validate_tls().unwrap_err();
        assert!(err.contains("require tls_enable = true"), "{err}");
    }

    #[test]
    fn tls_options_never_downgrade_to_plaintext() {
        let opts = ClientOpts {
            verify: Some(TlsVerify::Peer),
            ..Default::default()
        };
        let err = opts.validate_tls().unwrap_err();
        assert!(err.contains("require a CA bundle"), "{err}");
    }

    #[test]
    fn invalid_ca_and_server_name_values_are_rejected() {
        let empty = ClientOpts {
            tls_cacertfile: Some("  ".to_string()),
            ..tls_opts()
        };
        assert!(
            empty
                .validate_tls()
                .unwrap_err()
                .contains("must not be empty")
        );

        let relative = ClientOpts {
            tls_cacertfile: Some("relative/ca.pem".to_string()),
            ..tls_opts()
        };
        assert!(
            relative
                .validate_tls()
                .unwrap_err()
                .contains("absolute path")
        );

        let no_name = ClientOpts {
            tls_server_name: Some(String::new()),
            ..tls_opts()
        };
        assert!(
            no_name
                .validate_tls()
                .unwrap_err()
                .contains("tls_server_name must not be empty")
        );
    }

    #[test]
    fn server_name_defaults_to_host_and_can_be_overridden() {
        let mut opts = tls_opts();
        assert_eq!(opts.tls_server_name(), "127.0.0.1");
        opts.tls_server_name = Some("datalayers.example.com".to_string());
        assert_eq!(opts.tls_server_name(), "datalayers.example.com");
    }
}
