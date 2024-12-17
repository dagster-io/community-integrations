use std::{ffi::OsString, fs::OpenOptions, io::Write};

use thiserror::Error;

use crate::types::PipesMessage;

use super::StdStream;

/// Write messages back to the Dagster orchestration process.
/// To be used in conjunction with [`MessageWriter`](crate::MessageWriter).
pub trait MessageWriterChannel {
    /// Write a message to the orchestration process
    fn write_message(&mut self, message: PipesMessage) -> Result<(), MessageWriteError>;
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MessageWriteError {
    #[error("io error at destination {}: {}", .dst, .source)]
    #[non_exhaustive]
    IO {
        dst: String, // Write destination
        source: std::io::Error,
    },

    #[error(transparent)]
    Invalid(#[from] serde_json::Error),
}

#[derive(Debug, PartialEq)]
pub struct FileChannel {
    path: OsString,
}

impl FileChannel {
    pub fn new(path: OsString) -> Self {
        Self { path }
    }
}

impl MessageWriterChannel for FileChannel {
    fn write_message(&mut self, message: PipesMessage) -> Result<(), MessageWriteError> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|source| MessageWriteError::IO {
                dst: self.path.to_string_lossy().into_owned(),
                source,
            })?;
        let json = serde_json::to_string(&message)?;
        writeln!(file, "{json}").map_err(|source| MessageWriteError::IO {
            dst: self.path.to_string_lossy().into_owned(),
            source,
        })?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct StreamChannel {
    stream: StdStream,
}

impl StreamChannel {
    pub fn new(stream: StdStream) -> Self {
        Self { stream }
    }

    fn _format_message(message: &PipesMessage) -> Result<Vec<u8>, serde_json::Error> {
        let msg = format!("{}\n", serde_json::to_string(message)?).into_bytes();
        Ok(msg)
    }
}

impl MessageWriterChannel for StreamChannel {
    fn write_message(&mut self, message: PipesMessage) -> Result<(), MessageWriteError> {
        match self.stream {
            StdStream::Out => std::io::stdout()
                .write_all(&Self::_format_message(&message)?)
                .map_err(|source| MessageWriteError::IO {
                    dst: "stdout".to_string(),
                    source,
                }),
            StdStream::Err => std::io::stderr()
                .write_all(&Self::_format_message(&message)?)
                .map_err(|source| MessageWriteError::IO {
                    dst: "stderr".to_string(),
                    source,
                }),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct BufferedStreamChannel {
    buffer: Vec<PipesMessage>,
    stream: StdStream,
}

impl BufferedStreamChannel {
    pub fn new(stream: StdStream) -> Self {
        Self {
            buffer: vec![],
            stream,
        }
    }

    /// Flush messages in the buffer to the stream
    /// <div class="warning">This class will called once on `Drop`</div>
    fn flush(&mut self) -> Result<(), MessageWriteError> {
        let _: Vec<_> = self
            .buffer
            .iter()
            .map(|msg| match self.stream {
                StdStream::Out => std::io::stdout()
                    .write(&Self::_format_message(msg)?)
                    .map_err(|source| MessageWriteError::IO {
                        dst: "stdout".to_string(),
                        source,
                    }),
                StdStream::Err => std::io::stderr()
                    .write(&Self::_format_message(msg)?)
                    .map_err(|source| MessageWriteError::IO {
                        dst: "stderr".to_string(),
                        source,
                    }),
            })
            .collect();
        self.buffer.clear();
        Ok(())
    }

    fn _format_message(message: &PipesMessage) -> Result<Vec<u8>, serde_json::Error> {
        let msg = format!("{}\n", serde_json::to_string(message)?).into_bytes();
        Ok(msg)
    }
}

impl Drop for BufferedStreamChannel {
    /// Flush the data when out of scope or panicked.
    /// <div class="warning">Panic aborting will prevent `Drop` and this function from running</div>
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            eprintln!("Failed to flush buffer: {e}");
        }
    }
}

impl MessageWriterChannel for BufferedStreamChannel {
    fn write_message(&mut self, message: PipesMessage) -> Result<(), MessageWriteError> {
        self.buffer.push(message);
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum DefaultChannel {
    File(FileChannel),
    Stream(StreamChannel),
    BufferedStream(BufferedStreamChannel),
}

impl MessageWriterChannel for DefaultChannel {
    fn write_message(&mut self, message: PipesMessage) -> Result<(), MessageWriteError> {
        match self {
            Self::File(channel) => channel.write_message(message),
            Self::Stream(channel) => channel.write_message(message),
            Self::BufferedStream(channel) => channel.write_message(message),
        }
    }
}

#[cfg(test)]
mod tests_file_channel {
    use tempfile::NamedTempFile;

    use crate::{Method, PipesMessage};

    use super::{FileChannel, MessageWriterChannel};

    #[test]
    fn test_write_message() {
        let file = NamedTempFile::new().expect("Failed to create tempfile for testing");
        let mut channel = FileChannel::new(file.path().into());
        let message = PipesMessage::new(Method::Opened, None);
        channel
            .write_message(message.clone())
            .expect("Failed to write message");

        let file_content =
            std::fs::read_to_string(file.path()).expect("Failed to read from tempfile");
        assert_eq!(
            message,
            serde_json::from_str(&file_content).expect("Failed to serialize PipesMessage")
        );
    }
}
