use std::fmt;

#[derive(Debug)]
pub struct Error {
    pub code: u8,
    pub message: String,
}
pub type Result<T> = std::result::Result<T, Error>;
impl Error {
    pub fn new(code: u8, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(2, message)
    }
    pub fn input(message: impl Into<String>) -> Self {
        Self::new(3, message)
    }
    pub fn sql(message: impl Into<String>) -> Self {
        Self::new(4, message)
    }
    pub fn output(message: impl Into<String>) -> Self {
        Self::new(5, message)
    }
    pub fn context(mut self, context: impl fmt::Display) -> Self {
        self.message = format!("{context}: {}", self.message);
        self
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for Error {}
impl From<datafusion::error::DataFusionError> for Error {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Self::sql(e.to_string())
    }
}
