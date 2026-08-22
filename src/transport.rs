use crate::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub type Frame = Vec<u8>;

pub struct MessageHeader {
    version: u8,
    flags: u8,
    userid: u32,
    rolemask: u32,
    kind: MessageHeaderKind,
}

pub enum MessageHeaderKind {
    Request { nodeid: u32, matchtag: u32 },
    Response { errnum: u32, matchtag: u32 },
    Event { sequence: u32 },
    Control { type_: u32, status: u32 },
}

impl MessageHeader {
    pub fn new_request(
        nodeid: u32,
        matchtag: Option<u32>,
        with_payload: bool,
        upstream_flag: bool,
    ) -> MessageHeader {
        let mut flags: u8 = 0;
        flags |= 0x01; // All requests have a topic string
        if with_payload {
            flags |= 0x02; // payload lfag
        }
        if matchtag.is_none() {
            flags |= 0x04 // no-return flag
            // n.b. this flag needs to be reset, if we set the matchtag later
        }
        flags |= 0x08; // All requests and responses have a route delimiter frame (even if there are no route frames)
        if upstream_flag {
            flags |= 0x10; // flag-upstream
        }

        // TODO: add flag-streaming if we ever implement streaming rpc

        // These are the default setting when using the local connector
        // the receiving broker will set userid and rolemask for us.
        let userid = 0xFFFFFFFF;
        let rolemask = 0;

        Self {
            version: 1,
            flags,
            userid,
            rolemask,
            kind: MessageHeaderKind::Request {
                nodeid,
                matchtag: matchtag.unwrap_or(0),
            },
        }
    }

    pub fn set_matchtag(&mut self, new_matchtag: u32) {
        if let MessageHeaderKind::Request {
            nodeid: _,
            matchtag,
        } = &mut self.kind
        {
            *matchtag = new_matchtag;
            self.flags &= !0x04;
        }
    }

    async fn write_to_stream<W>(&self, stream: &mut W) -> Result<(), Error>
    where
        W: tokio::io::AsyncWriteExt + Unpin,
    {
        stream.write_u8(0x8E).await?; // magic cookie
        stream.write_u8(self.version).await?; // version
        let discriminant = match self.kind {
            MessageHeaderKind::Request { .. } => 0x01,
            MessageHeaderKind::Response { .. } => 0x02,
            MessageHeaderKind::Event { .. } => 0x04,
            MessageHeaderKind::Control { .. } => 0x08,
        };
        stream.write_u8(discriminant).await?;
        stream.write_u8(self.flags).await?;
        stream.write_u32(self.userid).await?;
        stream.write_u32(self.rolemask).await?;
        match self.kind {
            MessageHeaderKind::Request { nodeid, matchtag } => {
                stream.write_u32(nodeid).await?;
                stream.write_u32(matchtag).await?;
            }
            MessageHeaderKind::Response { errnum, matchtag } => {
                stream.write_u32(errnum).await?;
                stream.write_u32(matchtag).await?;
            }
            MessageHeaderKind::Event { sequence } => {
                stream.write_u32(sequence).await?;
                stream.write_u32(0).await?;
            }
            MessageHeaderKind::Control { type_, status } => {
                stream.write_u32(type_).await?;
                stream.write_u32(status).await?;
            }
        }

        Ok(())
    }

    fn try_from_frame(frame: &[u8]) -> Result<Self, Error> {
        if frame.len() != 20 {
            return Err(Error::DecodeError);
        }

        // magic cookie for message header
        if frame[0] != 0x8e {
            return Err(Error::DecodeError);
        }

        let version = frame[1];

        if version != 0x01 {
            return Err(Error::DecodeError);
        }

        let flags = frame[3];
        if flags > 127 {
            return Err(Error::DecodeError);
        }

        let userid = u32::from_be_bytes(frame[4..8].as_array::<4>().unwrap().to_owned());
        let rolemask = u32::from_be_bytes(frame[8..12].as_array::<4>().unwrap().to_owned());

        let kind_discriminant = frame[2];

        let kind = match kind_discriminant {
            0x01 => {
                let nodeid = u32::from_be_bytes(frame[12..16].as_array::<4>().unwrap().to_owned());
                let matchtag =
                    u32::from_be_bytes(frame[16..20].as_array::<4>().unwrap().to_owned());
                MessageHeaderKind::Request { nodeid, matchtag }
            }
            0x02 => {
                let errnum = u32::from_be_bytes(frame[12..16].as_array::<4>().unwrap().to_owned());
                let matchtag =
                    u32::from_be_bytes(frame[16..20].as_array::<4>().unwrap().to_owned());
                MessageHeaderKind::Response { errnum, matchtag }
            }
            0x04 => {
                let sequence =
                    u32::from_be_bytes(frame[12..16].as_array::<4>().unwrap().to_owned());
                MessageHeaderKind::Event { sequence }
            }
            0x08 => {
                let type_ = u32::from_be_bytes(frame[12..16].as_array::<4>().unwrap().to_owned());
                let status = u32::from_be_bytes(frame[16..20].as_array::<4>().unwrap().to_owned());
                MessageHeaderKind::Control { type_, status }
            }
            _ => {
                return Err(Error::DecodeError);
            }
        };

        Ok(MessageHeader {
            version,
            flags,
            userid,
            rolemask,
            kind,
        })
    }
    pub fn has_payload(&self) -> bool {
        self.flags & 0x02 != 0
    }
    pub fn is_response(&self) -> Option<(u32, u32)> {
        if let MessageHeaderKind::Response { errnum, matchtag } = self.kind {
            Some((errnum, matchtag))
        } else {
            None
        }
    }
}

pub(crate) type RawMessage = (MessageHeader, Vec<Frame>);

pub(crate) trait TransportReceive {
    async fn receive_message(&mut self) -> Result<RawMessage, Error>;
}

pub(crate) trait TransportSend {
    async fn send_message(&mut self, header: &MessageHeader, frames: &[Frame])
    -> Result<(), Error>;
}

mod usock {
    use std::path::PathBuf;
    use std::str::FromStr;

    use super::*;

    pub struct UsockTransportReceive(tokio::io::BufReader<tokio::net::unix::OwnedReadHalf>);

    impl TransportReceive for UsockTransportReceive {
        async fn receive_message(&mut self) -> Result<RawMessage, Error> {
            let total_message_size = self.receive_transport_header().await?;

            let mut received_size = 0;

            let mut received_frames = Vec::new();

            while received_size < total_message_size {
                let (new_frame, part_size) = self.receive_message_frame().await?;
                received_size += part_size;
                received_frames.push(new_frame);
            }

            let header_frame = received_frames.pop().ok_or(Error::DecodeError)?;
            let raw_header = MessageHeader::try_from_frame(header_frame.as_slice())?;
            Ok((raw_header, received_frames))
        }
    }

    impl UsockTransportReceive {
        async fn receive_transport_header(&mut self) -> Result<usize, Error> {
            let mut magic = [0; 4];
            self.0.read_exact(&mut magic).await?;
            // the magic cookie in RFC3 is in the wrong order
            if magic != [0x12, 0x00, 0xEE, 0xFF] {
                return Err(Error::DecodeError);
            }

            let size = self.0.read_u32().await?;

            if size < 1 {
                return Err(Error::DecodeError);
            }

            Ok(size as usize)
        }

        async fn receive_message_frame(&mut self) -> Result<(Frame, usize), Error> {
            let short_frame_size = self.0.read_u8().await?;
            let frame_size = if short_frame_size == 255 {
                u32::from_be(self.0.read_u32().await?) as usize
            } else {
                short_frame_size as usize
            };

            let mut frame = vec![0; frame_size];
            if frame_size > 0 {
                self.0.read_exact(&mut frame).await?;
            }

            // calculate the size of this message part (frame length + length fields)
            // as both is included in the total message size of the transport header
            let total_size = if frame_size < 255 {
                frame_size as usize + 1
            } else {
                frame_size as usize + 3
            };

            Ok((frame, total_size))
        }
    }

    pub struct UsockTransportSend(tokio::io::BufWriter<tokio::net::unix::OwnedWriteHalf>);

    impl TransportSend for UsockTransportSend {
        async fn send_message(
            &mut self,
            header: &MessageHeader,
            frames: &[Frame],
        ) -> Result<(), Error> {
            // calculate total message size
            let msg_size: u32 = frames
                .iter()
                .map(|f| f.len() as u32) // length of the message part
                .map(|l| if l < 255 { l + 1 } else { l + 5 }) // additional length of the length fields
                .sum::<u32>()
                + 20 // header
                + 1; // header length field

            // write transport header
            // contrary to the RFC, the local connector accepts the magic cookie only in reverse order
            self.0.write_all(&[0x12, 0x00, 0xee, 0xff]).await?;
            self.0.write_u32(msg_size).await?;

            // write non-msg-header frames
            for frame in frames {
                if frame.len() < 255 {
                    self.0.write_u8(frame.len() as u8).await?;
                } else {
                    self.0.write_u8(255).await?;
                    self.0.write_u32(frame.len() as u32).await?;
                }
                if !frame.is_empty() {
                    self.0.write_all(frame).await?;
                }
            }

            // write header frame
            self.0.write_u8(20).await?; // size of header
            header.write_to_stream(&mut self.0).await?;
            self.0.flush().await?;
            Ok(())
        }
    }

    pub async fn usock_transport(
        url: &str,
    ) -> Result<(UsockTransportSend, UsockTransportReceive), Error> {
        let url = url
            .strip_prefix("local://")
            .ok_or_else(|| Error::UnknownUrl(url.to_string()))?;
        let path = PathBuf::from_str(url).unwrap(); // Infallible

        let stream = tokio::net::UnixStream::connect(path)
            .await
            .map_err(Error::Connection)?;

        usock_transport_from_stream(stream).await
    }

    async fn usock_transport_from_stream(
        stream: tokio::net::UnixStream,
    ) -> Result<(UsockTransportSend, UsockTransportReceive), Error> {
        let (mut read_half, write_half) = stream.into_split();

        // check if our connections has worked
        let connection_errno = read_half.read_u8().await?;
        if connection_errno != 0 {
            return Err(Error::PermissionDenied(connection_errno));
        }

        Ok((
            UsockTransportSend(tokio::io::BufWriter::new(write_half)),
            UsockTransportReceive(tokio::io::BufReader::new(read_half)),
        ))
    }
}

pub use usock::{UsockTransportReceive, UsockTransportSend, usock_transport};
