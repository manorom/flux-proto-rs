#[derive(Debug)]
pub enum Error {
    DecodeError,
    PermissionDenied(u8),
    Connection(tokio::io::Error),
    Io(tokio::io::Error),
    UnknownUrl(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DecodeError => write!(f, "Could not decode Transport Header"),
            Self::PermissionDenied(errno) => {
                write!(f, "Permission denied by broker, errno={}", errno)
            }
            Self::Io(err) => write!(f, "I/O Error: {}", err),
            Self::Connection(err) => write!(f, "Connection Error: {}", err),
            Self::UnknownUrl(s) => write!(f, "FLUX URL with unsupported scheme: {}", s),
        }
    }
}

impl std::error::Error for Error {}

impl From<tokio::io::Error> for Error {
    fn from(value: tokio::io::Error) -> Self {
        Self::Io(value)
    }
}
