// Utilities implementing the Request/Response logic on top of the messages from
// `crate::transport`.
use crate::transport::Frame;

pub trait IntoPayload {
    fn into_payload(self) -> Option<Frame>;
}

impl IntoPayload for () {
    fn into_payload(self) -> Option<Frame> {
        None
    }
}

impl IntoPayload for Vec<u8> {
    fn into_payload(self) -> Option<Frame> {
        Some(self)
    }
}

impl IntoPayload for Option<Vec<u8>> {
    fn into_payload(self) -> Option<Frame> {
        self
    }
}

pub trait IntoTopic {
    fn into_topic(self) -> Frame;
}

impl IntoTopic for &str {
    fn into_topic(self) -> Frame {
        let mut topic = Vec::from(self.as_bytes());
        topic.push(b'\0');
        topic
    }
}


#[derive(Debug)]
pub struct Response {
    errnum: u32,
    topic: Vec<u8>,
    payload: Option<Vec<u8>>,
}

impl Response {
    pub fn new(errnum: u32, topic: Vec<u8>, payload: Option<Vec<u8>>) -> Self {
        Response {
            errnum,
            topic,
            payload,
        }
    }
    pub fn errno(&self) -> u32 {
        self.errnum
    }
    pub fn topic(&self) -> &[u8] {
        self.topic.as_slice()
    }
    pub fn payload_raw(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }
}
