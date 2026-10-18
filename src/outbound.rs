use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::protocol::ConnectorToGatewayMessage;

/// Maximum wire frame size (allows a 16 MiB body plus header metadata).
pub const MAX_WIRE_MESSAGE_BYTES: usize = 32 * 1024 * 1024;
pub const SEND_QUEUE_MAX_BYTES: usize = 32 * 1024 * 1024;

pub(crate) enum WirePayload {
    Text(String),
    Binary(Vec<u8>),
}

impl WirePayload {
    pub fn len(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Binary(bytes) => bytes.len(),
        }
    }
}

pub(crate) struct OutboundFrame {
    pub payload: WirePayload,
    // Account for a frame until its socket write finishes, including cancellation/errors.
    pub _permit: Option<OwnedSemaphorePermit>,
}

impl OutboundFrame {
    pub fn control(message: ConnectorToGatewayMessage) -> anyhow::Result<Self> {
        Ok(Self {
            payload: serialize(message)?,
            _permit: None,
        })
    }
}

#[derive(Clone)]
pub struct OutboundSender {
    inner: Sender,
}

#[derive(Clone)]
enum Sender {
    // Preserve direct forwarder use without requiring a live WebSocket writer.
    Direct(mpsc::Sender<ConnectorToGatewayMessage>),
    Wire {
        tx: mpsc::Sender<OutboundFrame>,
        budget: Arc<Semaphore>,
    },
}

impl From<mpsc::Sender<ConnectorToGatewayMessage>> for OutboundSender {
    fn from(tx: mpsc::Sender<ConnectorToGatewayMessage>) -> Self {
        Self {
            inner: Sender::Direct(tx),
        }
    }
}

fn serialize(message: ConnectorToGatewayMessage) -> anyhow::Result<WirePayload> {
    let payload = match message {
        ConnectorToGatewayMessage::HttpResponseChunk { stream_id, data } => {
            let header = serde_json::json!({
                "type": "http_response_chunk",
                "stream_id": stream_id,
            });
            let header_bytes = serde_json::to_vec(&header)?;
            let mut buf = Vec::with_capacity(header_bytes.len() + 1 + data.len());
            buf.extend_from_slice(&header_bytes);
            buf.push(b'\n');
            buf.extend_from_slice(&data);
            WirePayload::Binary(buf)
        }
        other => WirePayload::Text(serde_json::to_string(&other)?),
    };
    anyhow::ensure!(
        payload.len() <= MAX_WIRE_MESSAGE_BYTES,
        "WebSocket frame exceeds maximum size"
    );
    Ok(payload)
}

impl OutboundSender {
    pub(crate) fn channel(capacity: usize) -> (Self, mpsc::Receiver<OutboundFrame>) {
        Self::channel_with_budget(capacity, SEND_QUEUE_MAX_BYTES)
    }

    fn channel_with_budget(
        capacity: usize,
        byte_limit: usize,
    ) -> (Self, mpsc::Receiver<OutboundFrame>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                inner: Sender::Wire {
                    tx,
                    budget: Arc::new(Semaphore::new(byte_limit)),
                },
            },
            rx,
        )
    }

    pub async fn send(&self, message: ConnectorToGatewayMessage) -> anyhow::Result<()> {
        match &self.inner {
            Sender::Direct(tx) => tx
                .send(message)
                .await
                .map_err(|_| anyhow::anyhow!("WebSocket outbound channel closed")),
            Sender::Wire { tx, budget } => {
                let payload = serialize(message)?;
                let permit = budget
                    .clone()
                    .acquire_many_owned(payload.len() as u32)
                    .await?;
                tx.send(OutboundFrame {
                    payload,
                    _permit: Some(permit),
                })
                .await
                .map_err(|_| anyhow::anyhow!("WebSocket outbound channel closed"))
            }
        }
    }

    pub fn try_send(&self, message: ConnectorToGatewayMessage) -> anyhow::Result<()> {
        match &self.inner {
            Sender::Direct(tx) => tx
                .try_send(message)
                .map_err(|_| anyhow::anyhow!("WebSocket outbound queue full or closed")),
            Sender::Wire { tx, budget } => {
                let payload = serialize(message)?;
                let permit = budget
                    .clone()
                    .try_acquire_many_owned(payload.len() as u32)?;
                tx.try_send(OutboundFrame {
                    payload,
                    _permit: Some(permit),
                })
                .map_err(|_| anyhow::anyhow!("WebSocket outbound queue full or closed"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> ConnectorToGatewayMessage {
        ConnectorToGatewayMessage::HttpResponseEnd {
            stream_id: "stream".into(),
        }
    }

    #[tokio::test]
    async fn queued_and_inflight_frames_release_every_byte_permit() {
        let frame_bytes = serialize(message()).unwrap().len();
        let (sender, mut receiver) = OutboundSender::channel_with_budget(4, frame_bytes * 2);
        let Sender::Wire { budget, .. } = &sender.inner else {
            unreachable!()
        };
        for _ in 0..1024 {
            sender.send(message()).await.unwrap();
            sender.send(message()).await.unwrap();
            assert_eq!(budget.available_permits(), 0);
            assert!(sender.try_send(message()).is_err());
            let inflight = receiver.recv().await.unwrap();
            assert_eq!(
                budget.available_permits(),
                0,
                "In-flight writes still own their bytes"
            );
            drop(inflight);
            assert_eq!(budget.available_permits(), frame_bytes);
            drop(receiver.recv().await.unwrap());
            assert_eq!(budget.available_permits(), frame_bytes * 2);
        }
        sender.send(message()).await.unwrap();
        drop(receiver);
        assert!(sender.send(message()).await.is_err());
        assert_eq!(budget.available_permits(), frame_bytes * 2);
    }

    #[tokio::test]
    async fn cancelling_a_byte_budget_wait_does_not_leak_permits() {
        let frame_bytes = serialize(message()).unwrap().len();
        let (sender, mut receiver) = OutboundSender::channel_with_budget(4, frame_bytes);
        sender.send(message()).await.unwrap();
        let blocked_sender = sender.clone();
        let blocked = tokio::spawn(async move { blocked_sender.send(message()).await });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished());
        blocked.abort();
        assert!(blocked.await.unwrap_err().is_cancelled());
        drop(receiver.recv().await.unwrap());
        sender.try_send(message()).unwrap();
    }
}
