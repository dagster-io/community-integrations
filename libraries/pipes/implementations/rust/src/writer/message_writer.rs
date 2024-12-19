use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::writer::message_writer_channel::{
    BufferedStreamChannel, DefaultChannel, FileChannel, MessageWriterChannel, StreamChannel,
};
use crate::writer::StdStream;

mod private {
    pub struct Token; // To seal certain trait methods
}

/// Write messages back to Dagster, via its associated [`MessageWriterChannel`](crate::MessageWriterChannel).
pub trait MessageWriter {
    type Channel: MessageWriterChannel;

    /// Initialize a channel for writing messages back to Dagster.
    ///
    /// This method should takes the params passed by the orchestration-side
    /// `PipesMessageReader` and use them to construct and yield
    /// [`MessageWriterChannel`].
    fn open(&self, params: Map<String, Value>) -> Self::Channel;

    /// Return a payload containing information about the external process to be passed back to
    /// the orchestration process. This should contain information that cannot be known before
    /// the external process is launched.
    ///
    /// # Note
    /// This method is sealed — it should not be overridden by users.
    /// Instead, users should override [`Self::get_opened_extras`] to inject custom data.
    ///
    /// ```compile_fail
    /// # use serde_json::{Map, Value};
    /// # use dagster_pipes_rust::{MessageWriter, MessageWriterChannel, PipesMessage};
    /// #
    /// struct MyMessageWriter(u64);
    /// #
    /// # struct MyChannel;
    /// # impl MessageWriterChannel for MyChannel {
    /// #    fn write_message(&mut self, message: PipesMessage) {
    /// #        todo!()
    /// #    }
    /// # };
    ///
    /// impl MessageWriter for MyMessageWriter {
    /// #    type Channel = MyChannel;
    /// #
    /// #    fn open(&self, params: Map<String, Value>) -> Self::Channel {
    ///      // ...
    /// #          todo!()
    /// #    }
    /// }
    ///
    /// MyMessageWriter(42).get_opened_payload(private::Token); // use of undeclared crate or module `private`
    /// ```
    fn get_opened_payload(&self, _: private::Token) -> HashMap<&str, Option<Value>> {
        let mut extras = HashMap::new();
        extras.insert("extras", Some(Value::Object(self.get_opened_extras())));
        extras
    }

    /// Return arbitary reader-specific information to be passed back to the orchestration
    /// process. The information will be returned under the `extras` key of the initialization payload.
    fn get_opened_extras(&self) -> Map<String, Value> {
        Map::new()
    }
}

/// Public accessor to the sealed method
pub fn get_opened_payload(writer: &impl MessageWriter) -> HashMap<&str, Option<Value>> {
    writer.get_opened_payload(private::Token)
}

pub struct DefaultWriter;

impl DefaultWriter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageWriter for DefaultWriter {
    type Channel = DefaultChannel;

    fn open(&self, params: Map<String, Value>) -> Self::Channel {
        const FILE_PATH_KEY: &str = "path";
        const STDIO_KEY: &str = "stdio";
        const BUFFERED_STDIO_KEY: &str = "buffered_stdio";
        const STDERR: &str = "stderr";
        const STDOUT: &str = "stdout";
        //const INCLUDE_STDIO_IN_MESSAGES_KEY: &str = "include_stdio_in_messages";

        match (
            params.get(FILE_PATH_KEY),
            params.get(STDIO_KEY),
            params.get(BUFFERED_STDIO_KEY),
        ) {
            (Some(Value::String(path)), _, _) => {
                // TODO: This is a simplified implementation. Utilize `PipesLogWriter`
                DefaultChannel::File(FileChannel::new(path.into()))
            }
            (None, Some(Value::String(stream)), _) => match &*(stream.to_lowercase()) {
                STDOUT => DefaultChannel::Stream(StreamChannel::new(StdStream::Out)),
                STDERR => DefaultChannel::Stream(StreamChannel::new(StdStream::Err)),
                _ => panic!("Invalid stream provided for stdio writer channel"),
            },
            (None, None, Some(Value::String(stream))) => {
                // Once `PipesBufferedStreamMessageWriterChannel` is dropped, the buffered data is written
                match &*(stream.to_lowercase()) {
                    STDOUT => {
                        DefaultChannel::BufferedStream(BufferedStreamChannel::new(StdStream::Out))
                    }
                    STDERR => {
                        DefaultChannel::BufferedStream(BufferedStreamChannel::new(StdStream::Err))
                    }
                    _ => panic!("Invalid stream provided for buffered stdio writer channel"),
                }
            }
            _ => panic!("No way to write messages"),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::writer::message_writer_channel::{
        BufferedStreamChannel, FileChannel, StreamChannel,
    };

    #[test]
    fn test_open_with_file_path_key() {
        let writer = DefaultWriter;
        let params = serde_json::from_str(r#"{"path": "my-file-path"}"#)
            .expect("Failed to parse raw JSON string");
        assert_eq!(
            writer.open(params),
            DefaultChannel::File(FileChannel::new("my-file-path".into()))
        );
    }

    #[rstest]
    #[case("stdout", StdStream::Out)]
    #[case("stderr", StdStream::Err)]
    fn test_open_with_stdio_key(#[case] value: &str, #[case] stream: StdStream) {
        let writer = DefaultWriter;
        let Value::Object(params) = json!({"stdio": value}) else {
            panic!("Unexpected JSON type encountered")
        };
        assert_eq!(
            writer.open(params),
            DefaultChannel::Stream(StreamChannel::new(stream))
        );
    }

    #[rstest]
    #[case("stdout", StdStream::Out)]
    #[case("stderr", StdStream::Err)]
    fn test_open_with_buffered_stdio_key(#[case] value: &str, #[case] stream: StdStream) {
        let writer = DefaultWriter;
        let Value::Object(params) = json!({"buffered_stdio": value}) else {
            panic!("Unexpected JSON type encountered")
        };
        assert_eq!(
            writer.open(params),
            DefaultChannel::BufferedStream(BufferedStreamChannel::new(stream))
        );
    }

    #[test]
    fn test_open_prioritizes_file_path_over_everything_else() {
        let writer = DefaultWriter;
        let Value::Object(params) =
            json!({"path": "my-file-path", "stdio": "stdout", "buffered_stdio": "stderr"})
        else {
            panic!("Unexpected JSON type encountered")
        };
        assert_eq!(
            writer.open(params),
            DefaultChannel::File(FileChannel::new("my-file-path".into()))
        );
    }

    #[test]
    fn test_open_prioritizes_stream_over_buffered_stream() {
        let writer = DefaultWriter;
        let Value::Object(params) = json!({"stdio": "stdout", "buffered_stdio": "stderr"}) else {
            panic!("Unexpected JSON type encountered")
        };
        assert_eq!(
            writer.open(params),
            DefaultChannel::Stream(StreamChannel::new(StdStream::Out))
        );
    }
}
