use crate::{IntoPayload, IntoTopic};
// Utilities implementing the Request/Response logic on top of the messages from
// `crate::transport`.
use crate::error::Error;
use crate::match_tag::MatchTagPool;
use crate::transport::{MessageHeader, RawMessage};
use std::collections::HashMap;
use std::sync::Mutex;

pub(crate) enum ResponseChannel {
    Response(tokio::sync::oneshot::Sender<Result<Response, Error>>),
    StreamingResponse(tokio::sync::mpsc::UnboundedSender<Result<Response, Error>>),
}

struct ResponseRouterInner {
    matchtag_pool: MatchTagPool,
    routes: HashMap<u32, ResponseChannel>,
}

pub(crate) struct ResponseRouter(Mutex<ResponseRouterInner>);

impl ResponseRouter {
    pub fn new(pool_size: u32) -> Self {
        ResponseRouter(Mutex::new(ResponseRouterInner {
            matchtag_pool: MatchTagPool::new(pool_size),
            routes: HashMap::new(),
        }))
    }
    pub fn new_route(&self, resp_route: ResponseChannel) -> u32 {
        let mut inner = self.0.lock().unwrap();
        let new_tag = inner.matchtag_pool.alloc_tag();
        inner.routes.insert(new_tag, resp_route);
        new_tag
    }
    pub fn get_route(&self, tag: u32, keep_streaming: bool) -> Option<ResponseChannel> {
        let mut inner = self.0.lock().unwrap();
        let channel = inner.routes.remove(&tag)?;
        if let ResponseChannel::StreamingResponse(sender) = &channel
            && keep_streaming
        {
            inner
                .routes
                .insert(tag, ResponseChannel::StreamingResponse(sender.clone()));
            Some(ResponseChannel::StreamingResponse(sender.clone()))
        } else {
            inner.matchtag_pool.free_tag(tag);
            Some(channel)
        }
    }
    pub fn remove_route(&self, tag: u32) {
        let mut inner = self.0.lock().unwrap();
        let _ = inner.routes.remove(&tag);
    }
}

impl ResponseChannel {
    fn signal_error(self, error: Error) {
        match self {
            ResponseChannel::Response(sender) => {
                let _ = sender.send(Err(error));
            }
            ResponseChannel::StreamingResponse(sender) => {
                let _ = sender.send(Err(error));
            }
        }
    }
    pub(crate) fn response(self, errnum: u32, topic: Vec<u8>, payload: Option<Vec<u8>>) {
        match self {
            Self::Response(sender) => {
                let _ = sender.send(Ok(Response::new(errnum, topic, payload)));
            }
            Self::StreamingResponse(sender) => {
                let _ = sender.send(Ok(Response::new(errnum, topic, payload)));
            }
        }
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

pub(crate) fn new_request(
    nodeid: u32,
    matchtag: Option<u32>,
    route_upstream: bool,
    topic: impl IntoTopic,
    payload: impl IntoPayload,
) -> RawMessage {
    let topic = topic.into_topic();
    let payload = payload.into_payload();

    let header = MessageHeader::new_request(nodeid, matchtag, payload.is_some(), route_upstream);
    let mut additional_frames = Vec::with_capacity(3);
    additional_frames.push(Vec::new());
    additional_frames.push(topic);
    if let Some(payload) = payload {
        additional_frames.push(payload);
    }
    (header, additional_frames)
}
