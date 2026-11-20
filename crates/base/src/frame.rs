//! The frame codec: one JSON value per frame, behind a length prefix.
//!
//! # Wire format
//!
//! A frame is a 4-byte length in **big-endian** (network) byte order, followed by exactly that many
//! bytes of UTF-8 JSON encoding one serde value. There is no padding, no trailer and no version
//! byte; versioning belongs to the messages themselves.
//!
//! The same codec runs over iroh QUIC streams and over the station's Unix socket: anything that
//! implements tokio's [`AsyncRead`] / [`AsyncWrite`]. The size cap and the read deadline are
//! parameters, because they differ per channel: [`WIRE_FRAME_MAX`] for frames from a peer,
//! [`LOCAL_FRAME_MAX`] on the local socket.
//!
//! # Unknown fields
//!
//! The codec adds no schema rules of its own. Message types must tolerate unknown fields, so a
//! newer peer can add one without breaking an older one: do not put
//! `#[serde(deny_unknown_fields)]` on a type that is read from a frame.
//!
//! # Memory
//!
//! The body buffer of a read is exactly the announced length, which is never more than the cap. A
//! peer that sends a header and then stalls holds that buffer until the read deadline, so the
//! bound per stream is the cap. The cap does not bound what deserializing can allocate: a small
//! frame read into `serde_json::Value` can still expand into many nodes. Read peer frames into
//! concrete message types, not into `Value`.
//!
//! Non-finite floats (`NaN`, infinities) are written as JSON `null` and do not round-trip.
//!
//! # Cancel safety
//!
//! Neither [`read_frame`] nor [`write_frame`] is cancel-safe. Dropping a read in the middle of a
//! frame loses the bytes already consumed, and dropping a write may leave half a frame on the
//! stream. Either way the framing is lost.
//!
//! So never put `read_frame` in a `select!` branch that is re-created on every turn of a loop
//! (the natural shape of a subscription loop): when another branch wins in the middle of a frame,
//! the bytes already read are gone. Instead give the read half to a task of its own
//! (`UnixStream::into_split`; a QUIC receive stream is separate already) that reads frames in a
//! loop and hands them over a channel, and `select!` on the channel. To stop, abort that task and
//! close the stream.
//!
//! After a cancelled call, after [`FrameError::Timeout`] or [`FrameError::TooLarge`] on a read,
//! and after [`FrameError::Io`] on either side, the stream is unusable: close it instead of reading
//! or writing more frames on it.

use std::fmt;
use std::io;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Largest frame accepted from a peer over the network.
pub const WIRE_FRAME_MAX: usize = 64 * 1024;

/// Largest frame accepted on the station's local socket.
pub const LOCAL_FRAME_MAX: usize = 1024 * 1024;

/// Bytes in the length prefix.
const HEADER_LEN: usize = 4;

/// Why reading or writing a frame failed.
#[derive(Debug)]
pub enum FrameError {
    /// The frame is longer than the cap allows. On a read the body was not read: the stream is no
    /// longer usable and must be closed. On a write nothing was written.
    TooLarge {
        /// The length announced by the header, or the length of the serialized value.
        len: u64,
        /// The cap in force (on a write, never more than `u32::MAX`).
        cap: usize,
    },
    /// The stream ended cleanly before the first byte of a frame: the normal end of a stream of
    /// frames. An end in the middle of a frame is [`FrameError::Io`] instead.
    Closed,
    /// The read deadline passed before a whole frame arrived.
    Timeout,
    /// The bytes are not valid JSON for the expected type, or the value could not be serialized.
    Json(serde_json::Error),
    /// Any other I/O failure, including a stream that ended in the middle of a frame
    /// ([`io::ErrorKind::UnexpectedEof`]).
    Io(io::Error),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { len, cap } => write!(
                f,
                "frame of {len} bytes is larger than the limit of {cap} bytes for this connection; \
                 the connection must be closed (large data belongs in a byte stream or a blob, not \
                 in a message)"
            ),
            Self::Closed => f.write_str("the other side closed the stream"),
            Self::Timeout => f.write_str(
                "no complete frame arrived before the read deadline; the other side is stalled or \
                 too slow, and the connection must be closed",
            ),
            Self::Json(e) => write!(
                f,
                "frame does not hold the expected message ({e}); the other side may run an \
                 incompatible version"
            ),
            Self::Io(e) if e.kind() == io::ErrorKind::UnexpectedEof => f.write_str(
                "the stream ended in the middle of a frame; the other side went away or sent a \
                 truncated message",
            ),
            Self::Io(e) => write!(f, "I/O error on a framed stream: {e}"),
        }
    }
}

impl std::error::Error for FrameError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Json(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::TooLarge { .. } | Self::Closed | Self::Timeout => None,
        }
    }
}

impl From<io::Error> for FrameError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for FrameError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Serializes `value` as JSON and writes it as one frame, then flushes.
///
/// The value is serialized before anything is written. If its JSON is longer than `cap` (or than
/// `u32::MAX`), the call fails with [`FrameError::TooLarge`] and writes nothing, so a sender never
/// produces a frame its receiver must reject. Header and body go out in a single `write_all`.
///
/// There is no deadline parameter. Where a stalled reader matters (on the link, always), wrap the
/// call and treat an expiry like a read timeout; the stream is unusable afterwards:
///
/// ```no_run
/// # use std::time::Duration;
/// # use numbers_base::{FrameError, WIRE_FRAME_MAX, write_frame};
/// # async fn send<W: tokio::io::AsyncWrite + Unpin>(
/// #     send: &mut W, msg: &str, deadline: Duration,
/// # ) -> Result<(), FrameError> {
/// tokio::time::timeout(deadline, write_frame(send, msg, WIRE_FRAME_MAX))
///     .await
///     .map_err(|_elapsed| FrameError::Timeout)??;
/// # Ok(())
/// # }
/// ```
///
/// Not cancel-safe; see the [module docs](self).
pub async fn write_frame<W, T>(writer: &mut W, value: &T, cap: usize) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin + ?Sized,
    T: Serialize + ?Sized,
{
    let buf = encode_frame(value, cap)?;
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

/// Reads one frame and deserializes its JSON as `T`.
///
/// The length in the header is checked against `cap` before any of the body is read; a longer
/// frame fails with [`FrameError::TooLarge`]. The body buffer is then exactly the announced length,
/// never more than `cap`; a peer that sends a header and stalls holds it until the deadline, so the
/// bound per stream is the cap.
///
/// `deadline` is a budget for the whole call, header and body: with `Some(d)` the call fails with
/// [`FrameError::Timeout`] once `d` has passed. With `None` it waits as long as it takes; that is
/// meant only for an open subscription on the local socket, and every other caller passes a
/// deadline.
///
/// A stream that ends before the first byte of a frame gives [`FrameError::Closed`]; one that ends
/// inside a frame gives [`FrameError::Io`]. Not cancel-safe; see the [module docs](self).
///
/// # Panics
///
/// With `Some` deadline, panics if the tokio runtime was built without the time driver (neither
/// `enable_time` nor `enable_all`).
pub async fn read_frame<R, T>(
    reader: &mut R,
    cap: usize,
    deadline: Option<Duration>,
) -> Result<T, FrameError>
where
    R: AsyncRead + Unpin + ?Sized,
    T: DeserializeOwned,
{
    let body = match deadline {
        Some(d) => tokio::time::timeout(d, read_body(reader, cap))
            .await
            .map_err(|_elapsed| FrameError::Timeout)??,
        None => read_body(reader, cap).await?,
    };
    Ok(serde_json::from_slice(&body)?)
}

/// Serializes `value` into one buffer holding a whole frame, header and body.
///
/// For a caller that must know that a value fits before it opens a connection: write the buffer
/// as it is, and flush. [`write_frame`] is this followed by the write.
///
/// Fails with [`FrameError::TooLarge`] if the body is larger than `cap`, and with
/// [`FrameError::Json`] if `value` does not serialize.
pub fn encode_frame<T: Serialize + ?Sized>(value: &T, cap: usize) -> Result<Vec<u8>, FrameError> {
    let mut buf = vec![0u8; HEADER_LEN];
    serde_json::to_writer(&mut buf, value)?;
    let body_len = buf.len() - HEADER_LEN;
    let limit = cap.min(usize::try_from(u32::MAX).unwrap_or(usize::MAX));
    let len = match u32::try_from(body_len) {
        Ok(len) if body_len <= limit => len,
        _ => {
            return Err(FrameError::TooLarge {
                len: u64::try_from(body_len).unwrap_or(u64::MAX),
                cap: limit,
            });
        }
    };
    buf[..HEADER_LEN].copy_from_slice(&len.to_be_bytes());
    Ok(buf)
}

/// Reads the header, checks it against `cap`, then reads exactly the body.
async fn read_body<R>(reader: &mut R, cap: usize) -> Result<Vec<u8>, FrameError>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut header = [0u8; HEADER_LEN];
    let mut filled = 0;
    while filled < HEADER_LEN {
        let n = reader.read(&mut header[filled..]).await?;
        if n == 0 {
            return Err(if filled == 0 {
                FrameError::Closed
            } else {
                FrameError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream ended inside a frame header",
                ))
            });
        }
        filled += n;
    }
    let announced = u32::from_be_bytes(header);
    let len = match usize::try_from(announced) {
        Ok(len) if len <= cap => len,
        _ => {
            return Err(FrameError::TooLarge {
                len: u64::from(announced),
                cap,
            });
        }
    };
    // Exactly the announced length, already checked against the cap. `read_exact` reads no byte
    // past the frame, and an early end of the stream is `UnexpectedEof`.
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).await?;
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use tokio::io::duplex;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Hello {
        v: u32,
        name: String,
        tags: Vec<String>,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum Msg {
        Ping,
        Text { body: String },
        Bytes { data: Vec<u8> },
    }

    /// The bytes `write_frame` would put on the stream.
    async fn frame_bytes<T: Serialize + ?Sized>(value: &T, cap: usize) -> Vec<u8> {
        let mut out = Vec::new();
        write_frame(&mut out, value, cap).await.unwrap();
        out
    }

    /// A raw frame with the given body bytes.
    fn raw_frame(body: &[u8]) -> Vec<u8> {
        let mut out = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
        out.extend_from_slice(body);
        out
    }

    /// Awaits `fut`, failing the test instead of hanging if it never completes. With a paused clock
    /// a hung read leaves no timer but this one, so the runtime jumps to it at once.
    async fn guard<F: std::future::Future>(fut: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(3600), fut)
            .await
            .expect("read_frame hung: it should have returned long before the 1 h test guard")
    }

    /// A JSON string whose encoding is exactly `n` bytes (`n >= 2`).
    fn json_string_of_len(n: usize) -> String {
        "a".repeat(n - 2)
    }

    #[tokio::test]
    async fn round_trip_struct_and_enum_at_both_caps() {
        let hello = Hello {
            v: 1,
            name: "station \u{e9}\u{1f600}".into(),
            tags: vec!["a".into(), "b".into()],
        };
        let msgs = [
            Msg::Ping,
            Msg::Text { body: "hi".into() },
            Msg::Bytes {
                data: vec![0, 1, 255],
            },
        ];
        for cap in [WIRE_FRAME_MAX, LOCAL_FRAME_MAX] {
            let (mut a, mut b) = duplex(4096);
            write_frame(&mut a, &hello, cap).await.unwrap();
            for m in &msgs {
                write_frame(&mut a, m, cap).await.unwrap();
            }
            drop(a);
            let got: Hello = read_frame(&mut b, cap, None).await.unwrap();
            assert_eq!(got, hello);
            for m in &msgs {
                let got: Msg = read_frame(&mut b, cap, Some(Duration::from_secs(5)))
                    .await
                    .unwrap();
                assert_eq!(&got, m);
            }
            assert!(matches!(
                read_frame::<_, Msg>(&mut b, cap, None).await,
                Err(FrameError::Closed)
            ));
        }
    }

    #[tokio::test]
    async fn header_is_big_endian_length_of_the_json() {
        let bytes = frame_bytes(&Msg::Ping, WIRE_FRAME_MAX).await;
        let json = br#"{"type":"ping"}"#;
        assert_eq!(&bytes[..4], &[0, 0, 0, 15]);
        assert_eq!(&bytes[4..], json);
    }

    #[tokio::test]
    async fn frame_of_exactly_cap_passes_and_cap_plus_one_is_refused_unwritten() {
        let cap = 1000;
        let exact = json_string_of_len(cap);
        let bytes = frame_bytes(&exact, cap).await;
        assert_eq!(bytes.len(), HEADER_LEN + cap);
        let got: String = read_frame(&mut bytes.as_slice(), cap, None).await.unwrap();
        assert_eq!(got, exact);

        let over = json_string_of_len(cap + 1);
        let mut out = Vec::new();
        let err = write_frame(&mut out, &over, cap).await.unwrap_err();
        assert!(
            matches!(err, FrameError::TooLarge { len, cap: c } if len == 1001 && c == cap),
            "{err:?}"
        );
        assert!(out.is_empty(), "nothing may be written");

        // A frame of cap + 1 bytes on the stream is refused by the reader too.
        let raw = raw_frame(format!("\"{}\"", "a".repeat(cap - 1)).as_bytes());
        let err = read_frame::<_, String>(&mut raw.as_slice(), cap, None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, FrameError::TooLarge { len: 1001, .. }),
            "{err:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn oversize_header_is_refused_without_waiting_for_the_body() {
        let (mut a, mut b) = duplex(64);
        let announced = u32::try_from(WIRE_FRAME_MAX + 1).unwrap();
        a.write_all(&announced.to_be_bytes()).await.unwrap();
        // `a` stays open and the body never comes: a read of the body would wait forever. The
        // guard only turns such a hang into a test failure; `read_frame` has no deadline.
        let res = guard(read_frame::<_, Msg>(&mut b, WIRE_FRAME_MAX, None)).await;
        assert!(
            matches!(res, Err(FrameError::TooLarge { len, cap: WIRE_FRAME_MAX }) if len == u64::from(announced)),
            "{res:?}"
        );
        drop(a);
    }

    #[tokio::test(start_paused = true)]
    async fn u32_max_length_is_refused_from_the_header_alone() {
        let (mut a, mut b) = duplex(64);
        a.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
        let res = guard(read_frame::<_, Msg>(&mut b, 16, None)).await;
        assert!(
            matches!(res, Err(FrameError::TooLarge { len, cap: 16 }) if len == u64::from(u32::MAX)),
            "{res:?}"
        );
        drop(a);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_passes_with_nothing_arriving() {
        let (a, mut b) = duplex(64);
        let start = tokio::time::Instant::now();
        let res = guard(read_frame::<_, Msg>(
            &mut b,
            WIRE_FRAME_MAX,
            Some(Duration::from_secs(5)),
        ))
        .await;
        assert!(matches!(res, Err(FrameError::Timeout)), "{res:?}");
        assert!(start.elapsed() >= Duration::from_secs(5));
        drop(a);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_covers_a_stalled_body() {
        let (mut a, mut b) = duplex(64);
        let body = br#"{"type":"ping"}"#;
        a.write_all(&u32::try_from(body.len()).unwrap().to_be_bytes())
            .await
            .unwrap();
        a.write_all(&body[..5]).await.unwrap();
        let res = guard(read_frame::<_, Msg>(
            &mut b,
            WIRE_FRAME_MAX,
            Some(Duration::from_secs(5)),
        ))
        .await;
        assert!(matches!(res, Err(FrameError::Timeout)), "{res:?}");
        drop(a);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_is_a_budget_for_header_and_body_together() {
        let (mut a, mut b) = duplex(64);
        let frame = raw_frame(br#"{"type":"ping"}"#);
        let writer = tokio::spawn(async move {
            // The header comes after 3 s and the body 3 s later: each part alone is within a 5 s
            // deadline, the whole frame is not.
            tokio::time::sleep(Duration::from_secs(3)).await;
            a.write_all(&frame[..HEADER_LEN]).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
            a.write_all(&frame[HEADER_LEN..]).await.unwrap();
            a
        });
        let res = guard(read_frame::<_, Msg>(
            &mut b,
            WIRE_FRAME_MAX,
            Some(Duration::from_secs(5)),
        ))
        .await;
        assert!(matches!(res, Err(FrameError::Timeout)), "{res:?}");
        drop(guard(writer).await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn no_deadline_waits_for_data_that_arrives_late() {
        let (mut a, mut b) = duplex(64);
        let frame = frame_bytes(
            &Msg::Text {
                body: "late".into(),
            },
            WIRE_FRAME_MAX,
        )
        .await;
        let writer = tokio::spawn(async move {
            // Far longer than any read deadline, well within the test guard.
            tokio::time::sleep(Duration::from_secs(20 * 60)).await;
            a.write_all(&frame[..3]).await.unwrap();
            tokio::time::sleep(Duration::from_secs(20 * 60)).await;
            a.write_all(&frame[3..]).await.unwrap();
            a
        });
        let got: Msg = guard(read_frame(&mut b, WIRE_FRAME_MAX, None))
            .await
            .unwrap();
        assert_eq!(
            got,
            Msg::Text {
                body: "late".into()
            }
        );
        drop(guard(writer).await.unwrap());
    }

    #[tokio::test]
    async fn clean_eof_before_a_frame_is_closed() {
        let mut empty: &[u8] = &[];
        let res = read_frame::<_, Msg>(&mut empty, WIRE_FRAME_MAX, None).await;
        assert!(matches!(res, Err(FrameError::Closed)), "{res:?}");
    }

    #[tokio::test]
    async fn eof_inside_the_header_is_io_unexpected_eof() {
        let mut partial: &[u8] = &[0, 0];
        let res = read_frame::<_, Msg>(&mut partial, WIRE_FRAME_MAX, None).await;
        assert!(
            matches!(&res, Err(FrameError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof),
            "{res:?}"
        );
    }

    #[tokio::test]
    async fn eof_inside_the_body_is_io_unexpected_eof() {
        let frame = raw_frame(br#"{"type":"ping"}"#);
        let mut partial = &frame[..frame.len() - 1];
        let res = read_frame::<_, Msg>(&mut partial, WIRE_FRAME_MAX, None).await;
        assert!(
            matches!(&res, Err(FrameError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof),
            "{res:?}"
        );
    }

    #[tokio::test]
    async fn invalid_json_and_wrong_shape_are_json_errors() {
        for body in [
            &b"{not json"[..],
            &b"\xff\xfe"[..],
            br#"{"type":"ping"} trailing"#,
            br#"{"type":"no_such_variant"}"#,
            br#"{"v":"one","name":"x","tags":[]}"#,
            b"",
        ] {
            let frame = raw_frame(body);
            let res = read_frame::<_, Hello>(&mut frame.as_slice(), WIRE_FRAME_MAX, None).await;
            assert!(matches!(res, Err(FrameError::Json(_))), "{body:?}: {res:?}");
        }
    }

    #[tokio::test]
    async fn unknown_fields_are_tolerated() {
        let frame = raw_frame(br#"{"v":2,"name":"x","tags":[],"added_later":{"a":[1,2]}}"#);
        let got: Hello = read_frame(&mut frame.as_slice(), WIRE_FRAME_MAX, None)
            .await
            .unwrap();
        assert_eq!(
            got,
            Hello {
                v: 2,
                name: "x".into(),
                tags: vec![]
            }
        );
        let frame = raw_frame(br#"{"type":"text","body":"b","extra":true}"#);
        let got: Msg = read_frame(&mut frame.as_slice(), WIRE_FRAME_MAX, None)
            .await
            .unwrap();
        assert_eq!(got, Msg::Text { body: "b".into() });
    }

    #[tokio::test]
    async fn back_to_back_frames_are_read_separately() {
        let mut stream = frame_bytes(&Msg::Ping, WIRE_FRAME_MAX).await;
        stream.extend(frame_bytes(&Msg::Text { body: "two".into() }, WIRE_FRAME_MAX).await);
        stream.extend_from_slice(b"rest");
        let mut reader = stream.as_slice();
        let first: Msg = read_frame(&mut reader, WIRE_FRAME_MAX, None).await.unwrap();
        let second: Msg = read_frame(&mut reader, WIRE_FRAME_MAX, None).await.unwrap();
        assert_eq!(first, Msg::Ping);
        assert_eq!(second, Msg::Text { body: "two".into() });
        assert_eq!(reader, b"rest", "the reader must not consume past a frame");
    }

    #[tokio::test(start_paused = true)]
    async fn frame_then_raw_bytes_on_a_stream_leaves_the_raw_bytes_unread() {
        // The byte-stream shape: a header frame, then raw bytes, sent in small pieces.
        let (mut a, mut b) = duplex(7);
        let mut stream = frame_bytes(&Msg::Text { body: "hdr".into() }, WIRE_FRAME_MAX).await;
        stream.extend_from_slice(b"raw terminal bytes");
        let writer = tokio::spawn(async move {
            for chunk in stream.chunks(3) {
                a.write_all(chunk).await.unwrap();
            }
            // Dropping `a` here ends the stream.
        });
        let first: Msg = guard(read_frame(&mut b, WIRE_FRAME_MAX, None))
            .await
            .unwrap();
        assert_eq!(first, Msg::Text { body: "hdr".into() });
        let mut rest = Vec::new();
        guard(b.read_to_end(&mut rest)).await.unwrap();
        assert_eq!(rest, b"raw terminal bytes");
        guard(writer).await.unwrap();
    }

    #[tokio::test]
    async fn body_buffer_is_exactly_the_announced_length() {
        let cap = WIRE_FRAME_MAX;
        let frame = frame_bytes(&json_string_of_len(cap), cap).await;
        let body = read_body(&mut frame.as_slice(), cap).await.unwrap();
        assert_eq!(body.len(), cap);
        assert_eq!(body.capacity(), cap);
    }

    #[tokio::test]
    async fn unserializable_value_is_a_json_error_and_writes_nothing() {
        use std::collections::BTreeMap;
        let mut map = BTreeMap::new();
        map.insert(vec![1u8], 1u8); // JSON object keys must be strings
        let mut out = Vec::new();
        let res = write_frame(&mut out, &map, WIRE_FRAME_MAX).await;
        assert!(matches!(res, Err(FrameError::Json(_))), "{res:?}");
        assert!(out.is_empty());
    }

    #[test]
    fn errors_display_something_actionable() {
        let errs = [
            FrameError::TooLarge {
                len: 70000,
                cap: WIRE_FRAME_MAX,
            },
            FrameError::Closed,
            FrameError::Timeout,
            FrameError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "x")),
            FrameError::Io(io::Error::other("boom")),
        ];
        for e in errs {
            assert!(!e.to_string().is_empty());
        }
        assert!(
            FrameError::TooLarge {
                len: 70000,
                cap: WIRE_FRAME_MAX
            }
            .to_string()
            .contains("70000")
        );
    }
}
