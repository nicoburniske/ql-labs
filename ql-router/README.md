# ql-router

`ql-router` is a minimal authenticated TCP relay for QL peers. It proves which QIDs belong to each live connection, looks up the recipient, and forwards complete QL records unchanged. QL payloads remain end-to-end encrypted.

```text
peer A                         router                         peer B
  |--- TCP: IK + Confirm ------->|<----- TCP: IK + Confirm -----|
  |--- Attach + proof ---------->|<---- Attach + proof ---------|
  |                              |                              |
  |--- authenticated QL record ->| validate sender and route    |
  |                              |--- authenticated QL record ->|
```

The transport has two phases. The handshake phase exchanges exact, length-delimited QL `Ik1` and `Ik2` records. The secure phase uses this frame:

```text
u32-le length | u8 kind | payload | 16-byte AES-GCM tag
```

The tag authenticates the length, kind, and complete payload. Each direction has an implicit counter used as its nonce; TCP ordering makes packet numbers and replay windows unnecessary. The client proves it derived the transport keys with an empty `Confirm` at counter zero. This permits Desktop Relay to stay connected before it has a Passport identity to attach. Client attachments and records then start at counter one, while router records start at counter zero.

Malformed control messages, invalid tags, unexpected kinds, and counter exhaustion close the connection. Frames are bounded before their bodies are allocated. A local size error does not consume a counter, so the caller may correct it and continue. I/O errors are terminal because a partial TCP write cannot be retried safely.

The `protocol` module is synchronous and runtime-independent. It owns handshake transitions, incremental frame assembly, authentication, nonce state, and the per-connection attach and routing state machine. Callers feed socket reads into `FrameDecoder::buffer` and report their length with `FrameDecoder::advance`; partial state survives cancellation in an outer runtime. `RouterConnection` emits attach, authentication, forwarding, and rejection actions for any outer server implementation. The optional `tokio` feature provides the existing async client and socket adapter.

After connecting, a client attaches a peer bundle and answers a challenge from that identity. The route is published only after the acceptance record is queued. The router only accepts records whose sender QID was authenticated on the same connection. A connection may own up to 64 routes. If the same QID attaches on another connection, the newest connection receives its records; every connection that proved the identity may still send from it. A superseded connection must reattach before it can receive again.

The server uses Tokio's multi-thread runtime with one lightweight task per connection. This already distributes sockets across a worker per available core without paying for an operating-system thread per peer. Router state is allocated once for the process lifetime and borrowed directly by connection tasks. Connections and pending transports are bounded, and transient accept exhaustion is retried. `QL_ROUTER_MAX_CONNECTIONS` changes the default 16,384-connection limit. Each connection uses one file descriptor, so the process limit must be raised accordingly. Connected clients may remain idle before attaching and still count toward the limit.

TCP carries setup, route attachment, and QL records using length-delimited frames. Each record is limited to 8 KiB. Per-connection queues are bounded and apply backpressure when a recipient is slower than its senders. Partial frames and stalled writes have deadlines. Records for missing or disconnected recipients are dropped; QL remains responsible for end-to-end session recovery.
