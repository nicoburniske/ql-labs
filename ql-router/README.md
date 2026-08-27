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

After connecting, a client attaches a peer bundle and answers a challenge from that identity. The route is published only after the acceptance record is queued. The router only accepts records whose sender QID was authenticated on the same connection. A connection may own up to 64 routes.

TCP carries setup, route attachment, and QL records using length-delimited frames. Each record is limited to 8 KiB. Per-recipient queues are bounded and apply backpressure when a recipient is slower than its senders. Records for missing or disconnected recipients are dropped; QL remains responsible for end-to-end session recovery.
