use crate::error::Error;
use crate::match_tag::MatchTagPool;
use crate::rpc::{IntoPayload, IntoTopic, Response};
use crate::transport::{
    MessageHeader, TransportReceive, TransportSend, UsockTransportReceive, UsockTransportSend,
    usock_transport,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

struct RouteTableInner {
    routes: HashMap<u32, ResponseChannel>,
    tag_pool: MatchTagPool,
}

#[derive(Clone)]
struct RouteTable(Arc<Mutex<RouteTableInner>>);

impl RouteTable {
    fn new(pool_size: u32) -> RouteTable {
        Self(Arc::new(Mutex::new(RouteTableInner {
            routes: HashMap::new(),
            tag_pool: MatchTagPool::new(pool_size),
        })))
    }
    fn add_route(&self, channel: ResponseChannel) -> u32 {
        let mut inner = self.0.lock().unwrap();
        let new_tag = inner.tag_pool.alloc_tag();
        inner.routes.insert(new_tag, channel);
        new_tag
    }
    fn get_route(&self, tag: u32, keep_streaming: bool) -> Option<ResponseChannel> {
        let mut inner = self.0.lock().unwrap();
        let channel = inner.routes.remove(&tag)?;
        if keep_streaming {
            match channel {
                ResponseChannel::Streaming(sender) => {
                    inner
                        .routes
                        .insert(tag, ResponseChannel::Streaming(sender.clone()));
                    Some(ResponseChannel::Streaming(sender))
                }
                channel => {
                    inner.tag_pool.free_tag(tag);
                    Some(channel)
                }
            }
        } else {
            inner.tag_pool.free_tag(tag);
            Some(channel)
        }
    }
}

enum ResponseChannel {
    Success(tokio::sync::oneshot::Sender<Result<(), Error>>),
    Single(tokio::sync::oneshot::Sender<Result<Response, Error>>),
    Streaming(tokio::sync::mpsc::UnboundedSender<Result<Response, Error>>),
}

impl ResponseChannel {
    fn signal_error(self, error: Error) {
        match self {
            ResponseChannel::Success(sender) => {
                let _ = sender.send(Err(error));
            }
            ResponseChannel::Single(sender) => {
                let _ = sender.send(Err(error));
            }
            ResponseChannel::Streaming(unbounded_sender) => {
                let _ = unbounded_sender.send(Err(error));
            }
        }
    }
    fn response(self, errnum: u32, topic: Vec<u8>, payload: Vec<u8>) {
        match self {
            Self::Success(..) => (),
            Self::Single(sender) => {
                let _ = sender.send(Ok(Response::new(errnum, topic, Some(payload))));
            }
            Self::Streaming(sender) => {
                let _ = sender.send(Ok(Response::new(errnum, topic, Some(payload))));
            }
        }
    }
}

struct ReactorRequest {
    nodeid: u32,
    topic: Vec<u8>,
    payload: Option<Vec<u8>>,
    route_upstream: bool,
    response: ResponseChannel,
}

impl ReactorRequest {
    fn message_header(&self, matchtag: Option<u32>) -> MessageHeader {
        MessageHeader::new_request(
            self.nodeid,
            matchtag,
            self.payload.is_some(),
            self.route_upstream,
        )
    }
}

mod reactor_impl {
    use super::*;

    pub(super) async fn start_receiving(
        transport_rx: UsockTransportReceive,
        recv_table: RouteTable,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut transport_rx = transport_rx;
            let recv_table = recv_table;

            while let Ok((msg_header, mut msg_frames)) = transport_rx.receive_message().await {
                if let Some((errno, matchtag)) = msg_header.is_response() {
                    // dispatch response
                    // get_route will automatically free the matchtag, unless it is a streaming response
                    let response_channel_sender = recv_table.get_route(matchtag, true);
                    if let Some(sender) = response_channel_sender {
                        // TODO: destructure with error checking
                        let payload = msg_frames.pop().unwrap();
                        let topic = msg_frames.pop().unwrap();
                        sender.response(errno, topic, payload);
                    }
                }
            }
        })
    }

    pub(super) async fn start_sending(
        send_queue_rx: tokio::sync::mpsc::UnboundedReceiver<ReactorRequest>,
        transport_tx: UsockTransportSend,
        recv_table: RouteTable,
    ) -> tokio::task::JoinHandle<()> {
        enum ResponseHandle {
            Matchtag(u32),
            SuccessChannel(tokio::sync::oneshot::Sender<Result<(), Error>>),
        }

        tokio::spawn(async move {
            let recv_table = recv_table;
            let mut send_queue_rx = send_queue_rx;
            let mut transport_tx = transport_tx;

            while let Some(raw_request) = send_queue_rx.recv().await {
                let mut header = raw_request.message_header(None); // we set the matchtag later
                let response_channel_writer = raw_request.response;
                let topic_frame = raw_request.topic;
                let payload_frame = raw_request.payload;
                let mut frames = vec![Vec::new(), topic_frame];
                if let Some(payload_frame) = payload_frame {
                    frames.push(payload_frame);
                }

                let response_handle =
                    if let ResponseChannel::Success(channel) = response_channel_writer {
                        ResponseHandle::SuccessChannel(channel)
                    } else {
                        let matchtag = recv_table.add_route(response_channel_writer);
                        header.set_matchtag(matchtag);
                        ResponseHandle::Matchtag(matchtag)
                    };

                if let Err(e) = transport_tx.send_message(&header, &frames).await {
                    match response_handle {
                        ResponseHandle::Matchtag(matchtag) => {
                            if let Some(channel) = recv_table.get_route(matchtag, false) {
                                channel.signal_error(e);
                            }
                        }
                        ResponseHandle::SuccessChannel(channel) => {
                            let _ = channel.send(Err(e)); // if the sending task does no longer care about the result, that's their problem
                        }
                    }
                } else if let ResponseHandle::SuccessChannel(channel) = response_handle {
                    let _ = channel.send(Ok(()));
                }
            }
        })
    }
}

type SendQueueSender = tokio::sync::mpsc::UnboundedSender<ReactorRequest>;

pub struct Reactor {
    send_queue_sender: SendQueueSender,
    sender_task: tokio::task::JoinHandle<()>,
    receiver_task: tokio::task::JoinHandle<()>,
}

impl Reactor {
    pub async fn run_connect_local(flux_uri: &str) -> Result<Reactor, Error> {
        let (send_queue_sender, send_queue_receiver) =
            tokio::sync::mpsc::unbounded_channel::<ReactorRequest>();
        let recv_table = RouteTable::new(8);
        let (transport_tx, transport_rx) = usock_transport(flux_uri).await?;

        let receiver_task = reactor_impl::start_receiving(transport_rx, recv_table.clone()).await;
        let sender_task =
            reactor_impl::start_sending(send_queue_receiver, transport_tx, recv_table).await;

        Ok(Reactor {
            send_queue_sender,
            sender_task,
            receiver_task,
        })
    }
    pub fn handle(&self) -> FluxHandle {
        FluxHandle(self.send_queue_sender.clone())
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        self.sender_task.abort();
        self.receiver_task.abort();
    }
}

#[derive(Clone)]
pub struct FluxHandle(SendQueueSender);

impl FluxHandle {
    pub async fn request(
        &self,
        nodeid: u32,
        topic: impl IntoTopic,
        payload: impl IntoPayload,
        route_upstream: bool,
    ) -> Result<(), Error> {
        let (response_channel_sender, response_channel_receiver) = tokio::sync::oneshot::channel();
        self.0
            .send(ReactorRequest {
                nodeid,
                topic: topic.into_topic(),
                payload: payload.into_payload(),
                route_upstream,
                response: ResponseChannel::Success(response_channel_sender),
            })
            .expect("Trying to send message with stopped reactor");

        response_channel_receiver
            .await
            .expect("Reactor terminated wth sent messages pending")
    }

    pub async fn request_with_response(
        &self,
        nodeid: u32,
        topic: impl IntoTopic,
        payload: impl IntoPayload,
        route_upstream: bool,
    ) -> Result<Response, Error> {
        let (response_channel_sender, response_channel_receiver) = tokio::sync::oneshot::channel();
        self.0
            .send(ReactorRequest {
                nodeid,
                topic: topic.into_topic(),
                payload: payload.into_payload(),
                route_upstream,
                response: ResponseChannel::Single(response_channel_sender),
            })
            .expect("Trying to send message with stopped reactor");

        response_channel_receiver
            .await
            .expect("Reactor terminated with sent messages pending")
    }
}
