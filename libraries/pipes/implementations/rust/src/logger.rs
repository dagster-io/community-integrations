use std::collections::HashMap;

use serde_json::json;

use crate::{
    types::{Method, PipesLogLevel, PipesMessage},
    writer::{message_writer::MessageWriter, message_writer_channel::MessageWriteError},
    MessageWriterChannel,
};

#[derive(Debug)]
pub struct PipesLogger<W>
where
    W: MessageWriter,
{
    message_channel: W::Channel,
}

impl<W> PipesLogger<W>
where
    W: MessageWriter,
{
    pub fn new(message_channel: W::Channel) -> Self {
        Self { message_channel }
    }

    fn _log_message(
        &mut self,
        message: &str,
        level: PipesLogLevel,
    ) -> Result<(), MessageWriteError> {
        let params = HashMap::from([
            ("message", Some(json!(message))),
            ("level", Some(json!(level))),
        ]);
        let closed_message = PipesMessage::new(Method::Log, Some(params));
        self.message_channel.write_message(closed_message)
    }

    pub fn critical(&mut self, message: &str) -> Result<(), MessageWriteError> {
        self._log_message(message, PipesLogLevel::Critical)
    }

    pub fn debug(&mut self, message: &str) -> Result<(), MessageWriteError> {
        self._log_message(message, PipesLogLevel::Debug)
    }

    pub fn error(&mut self, message: &str) -> Result<(), MessageWriteError> {
        self._log_message(message, PipesLogLevel::Error)
    }

    pub fn info(&mut self, message: &str) -> Result<(), MessageWriteError> {
        self._log_message(message, PipesLogLevel::Info)
    }

    pub fn warning(&mut self, message: &str) -> Result<(), MessageWriteError> {
        self._log_message(message, PipesLogLevel::Warning)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use std::fs;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::writer::{
        message_writer::DefaultWriter,
        message_writer_channel::{DefaultChannel, FileChannel},
    };

    #[rstest]
    #[case(PipesLogLevel::Critical)]
    #[case(PipesLogLevel::Debug)]
    #[case(PipesLogLevel::Error)]
    #[case(PipesLogLevel::Info)]
    #[case(PipesLogLevel::Warning)]
    fn test_logger(#[case] level: PipesLogLevel) {
        let file = NamedTempFile::new().unwrap();
        let channel = DefaultChannel::File(FileChannel::new(file.path().into()));
        let mut logger = PipesLogger::<DefaultWriter>::new(channel);

        match level {
            PipesLogLevel::Critical => logger.critical("message").unwrap(),
            PipesLogLevel::Debug => logger.debug("message").unwrap(),
            PipesLogLevel::Error => logger.error("message").unwrap(),
            PipesLogLevel::Info => logger.info("message").unwrap(),
            PipesLogLevel::Warning => logger.warning("message").unwrap(),
        }

        let json =
            serde_json::from_str::<serde_json::Value>(&fs::read_to_string(file.path()).unwrap())
                .unwrap();

        assert!(json.get("method").is_some_and(|m| m == "log"));
        assert!(json
            .get("params")
            .is_some_and(|p| p.get("message").is_some_and(|m| m == "message")));
        assert!(json.get("params").is_some_and(|p| p
            .get("level")
            .is_some_and(|l| *l == serde_json::to_value(level).unwrap())));
    }
}
