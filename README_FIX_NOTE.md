# Direct original-format stream fix

This branch fixes the Android relay startup crash and removes M3U as the primary output.

- Relay initialization runs on a background worker instead of the Android main thread.
- Actual exception class/message is shown instead of `error: null`.
- Source probing uses `Range: bytes=0-0` GET rather than HEAD-first probing.
- The app exposes a direct `/stream.<ext>` URL when the original container can be detected (for example `.mkv` or `.mp4`).
- Media bytes are relayed unchanged; there is no transcoding or remuxing.
- Rolling cache remains hard-capped at 2 GB.
- HTTP Range playback/seeking remains supported.
