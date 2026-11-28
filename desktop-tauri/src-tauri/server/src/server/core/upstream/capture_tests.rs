use super::*;
use super::sse::FramePolicy;
use crate::server::core::debug_traffic::TrafficCapture;
use futures::StreamExt;

const ANTHROPIC: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_test\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n"
);
const CHAT: &str = concat!(
    "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\n",
    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n"
);

fn response(body: &'static str) -> reqwest::Response {
    axum::http::Response::builder()
        .status(200)
        .header("content-type", "text/event-stream")
        .body(body)
        .unwrap()
        .into()
}

fn telemetry() -> (Arc<usage::RequestTelemetry>, Arc<TrafficCapture>) {
    let capture = Arc::new(TrafficCapture::begin("capture-test"));
    let telemetry = Arc::new(usage::RequestTelemetry::new());
    telemetry.set_capture(capture.clone());
    (telemetry, capture)
}

async fn drain(mut stream: ForwardStream) -> String {
    let mut output = Vec::new();
    while let Some(frame) = stream.next().await {
        output.extend_from_slice(&frame.unwrap());
    }
    String::from_utf8(output).unwrap()
}

#[tokio::test]
async fn translated_responses_capture_only_original_anthropic_bytes() {
    for streaming in [false, true] {
        let (telemetry, capture) = telemetry();
        let translated = Box::pin(translate::AnthropicToChatStream::new(
            response(ANTHROPIC),
            "test-model",
            &telemetry,
        ));
        if streaming {
            let stream = ForwardStream::from_translated(
                translated,
                None,
                ConnectionGuard::new(Connections::new()),
                telemetry,
                FramePolicy::default(),
            );
            assert!(drain(stream).await.contains("hello"));
        } else {
            let result = aggregate::aggregate_frame_stream(translated, telemetry, FramePolicy::default())
                .await
                .unwrap();
            assert_eq!(result.body["choices"][0]["message"]["content"], "hello");
        }
        assert_eq!(capture.captured_body(), ANTHROPIC);
    }
}

#[tokio::test]
async fn direct_chat_responses_still_capture_original_bytes_once() {
    for streaming in [false, true] {
        let (telemetry, capture) = telemetry();
        if streaming {
            let stream = ForwardStream::new(
                response(CHAT),
                None,
                ConnectionGuard::new(Connections::new()),
                telemetry,
                FramePolicy::default(),
            );
            assert!(drain(stream).await.contains("hello"));
        } else {
            let result = aggregate::aggregate_sse_completion(response(CHAT), telemetry, FramePolicy::default())
                .await
                .unwrap();
            assert_eq!(result.body["choices"][0]["message"]["content"], "hello");
        }
        assert_eq!(capture.captured_body(), CHAT);
    }
}

#[tokio::test]
async fn first_reasoning_chunk_flushes_immediately_and_subsequent_short_chunk_flushes_within_window() {
    let telemetry = Arc::new(usage::RequestTelemetry::new());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<Bytes, std::io::Error>>();
    let incoming = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
    let mut stream = ForwardStream::from_translated(
        Box::pin(incoming),
        None,
        ConnectionGuard::new(Connections::new()),
        telemetry,
        FramePolicy::default(),
    );

    tx.send(Ok(Bytes::from_static(
        b"data: {\"id\":\"c1\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"a\"}}]}\n\n",
    )))
    .unwrap();
    let first = tokio::time::timeout(Duration::from_millis(25), stream.next())
        .await
        .expect("first reasoning chunk should bypass coalescing")
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&first).contains("\"reasoning_content\":\"a\""));

    tx.send(Ok(Bytes::from_static(
        b"data: {\"id\":\"c1\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"bc\"}}]}\n\n",
    )))
    .unwrap();
    let second = tokio::time::timeout(Duration::from_millis(200), stream.next())
        .await
        .expect("buffered short reasoning chunk should flush on time window even while upstream is idle")
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&second).contains("\"reasoning_content\":\"bc\""));
    drop(tx);
    assert!(stream.next().await.is_none());
}
