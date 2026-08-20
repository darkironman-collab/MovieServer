# Extreme Stream Relay (Android MVP)

Android phone downloads/caches a direct HTTP/HTTPS media file and exposes the **original media stream** to a laptop on the same Wi-Fi/LAN.

There is no transcoding or remuxing. If the source is MKV, MP4, WebM, etc., the relay keeps the original bytes and content type.

## Direct stream URL

After starting the relay, the app shows a URL such as:

- `http://PHONE_IP:8787/stream.mkv`
- `http://PHONE_IP:8787/stream.mp4`
- `http://PHONE_IP:8787/stream` when the container cannot be identified from the source URL/content type

Health check:

- `http://PHONE_IP:8787/health`

The old M3U output is no longer the primary playback method. Use the **Direct Stream URL** shown in the Android app directly in Energy Media Player.

## Features

- Manual Paste Link
- Auto Paste on app open/resume
- Optional Auto Start after a valid clipboard URL
- Cache choices: 256 MB, 512 MB, 1 GB, 1.5 GB, 2 GB
- Hard 2 GB cache ceiling
- 2/4/8/12/16 prefetch threads
- 4 MB range chunks
- LRU rolling cache eviction
- HTTP Range support for player seeking
- Foreground service + partial wake lock
- Direct original-format stream URL shown and copyable in the app
- Real network/HTTP error details instead of `error: null`
- Range-GET source probing instead of HEAD-first probing
- GitHub Actions APK build

## Important limitations

1. Designed for direct media URLs whose origin supports HTTP byte-range requests. Large files without Range support are rejected with a clear error because multi-thread chunk relay requires byte ranges.
2. DRM/login-only streams and links requiring cookies, Referer headers, authorization headers, or other custom request headers may not work yet.
3. For a stable laptop URL, reserve the phone's Wi-Fi IP in the router (DHCP reservation) or assign a stable IP. Otherwise the phone IP can change after reconnect/reboot.
4. Android may show a clipboard access notice when Auto Paste reads the clipboard. This is normal OS privacy behavior.
5. Cache is temporary app cache and can be cleared by Android under storage pressure.

## Build APK on GitHub

1. Open **Actions** → **Build Android APK**.
2. Run the workflow or push to a configured build branch.
3. Download the artifact named `ExtremeStreamRelay-debug`.
4. Install `app-debug.apk` on the phone.

## Laptop / Energy Media Player

1. Phone and laptop must be on the same Wi-Fi/LAN.
2. Paste the original movie URL in the Android app.
3. Tap **Start / Update Stream**.
4. Wait until status shows `running on port 8787`.
5. Tap **Copy Direct Stream URL**.
6. Open that URL directly in Energy Media Player.

## Cache behavior

The movie can be much larger than 2 GB. The app stores 4 MB chunks and keeps the configured rolling cache limit. Older least-recently-used chunks are deleted as newer playback data is fetched.
