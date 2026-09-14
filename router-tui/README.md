# router-tui

A single QL peer with pairing, echo, and the same generated download benchmark
used by KeyOS `gui-app-qlv2`. All peer traffic goes through the router.

From the workspace root, in Kitty or Ghostty:

```sh
cargo run -p router-tui -- router.foundation.xyz:7447 ../foundation_app/bundle.bin
```

Arguments are optional and default to `router.foundation.xyz:7447` and `bundle.bin`.
Use the workspace's pinned nightly toolchain. Blit comes from Git `master` and
the lockfile records the tested commit. Allow roughly 80 columns and 30 rows
to show the pairing image and all controls together. The image occupies a fixed
28 × 14 cell slot, including while loading and after pairing.

1. Connect Prime to foundation_app and wait for KeyOS to log `router ready`.
2. Open the updated QL v2 Test app on Prime and choose **Scan Peer QR**.
3. Scan the TUI image. Prime pairs through the router and selects the new peer.
4. Use either side's echo/download controls. The TUI requests only the debug
   app permission needed for its requests to Prime. The QR contains only the
   hex-encoded pairing invite, not an onboarding URL.

The TUI serves `RequestEcho` and `DownloadBenchmark` to Prime. Its buttons call
`RequestPassportEcho` and `DownloadPassportBenchmark` on Prime. The local download
field defaults to 256 KiB. Downloads check the length and SHA-256 and discard the bytes.
Downloads served to Prime are limited to 16 MiB. These are generated benchmark
bytes, not access to either device's filesystem.

The app owns the terminal event loop using public `Session` and `Frame` APIs.
Blit's local executor polls UI futures, a socket wakes the loop when tasks are
ready, and Tokio supplies networking and timers. No changes to blit are needed.

MVP limits: one Prime, fresh identity on each run, no saved pairing and no
automatic reconnect. Restart and scan again after a connection failure.
Use Tab to select fields, Enter to submit, or click a field to focus it.
Quit with the button or Ctrl-C.
Physical camera scanning still needs a device test.
