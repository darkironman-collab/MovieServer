# Extreme Stream Relay (Android MVP)

Android phone downloads/caches a direct HTTP/HTTPS media file and exposes it to a laptop on the same LAN via a permanent local M3U endpoint.

## Main URLs

- `http://PHONE_IP:8787/permanent.m3u`
- `http://PHONE_IP:8787/play`
- `http://PHONE_IP:8787/health`

The M3U endpoint always points to `/play`. Change the source movie inside the Android app; the laptop playlist does not need to change as long as the phone keeps the same LAN IP.

## Features in v0.1 MVP

- Manual Paste Link
- Auto Paste on app open/resume
- Optional Auto Start after valid clipboard URL
- Cache choices: 256 MB, 512 MB, 1 GB, 1.5 GB, 2 GB
- Hard 2 GB cache ceiling
- 2/4/8/12/16 prefetch threads
- 4 MB range chunks
- LRU rolling cache eviction
- HTTP Range support for player seeking
- Foreground service + partial wake lock
- Current M3U/play URL shown in app
- GitHub Actions APK build

## Important limitations

1. v0.1 is designed for direct media URLs whose origin supports HTTP byte-range requests. DRM/login-only streams and links requiring expiring cookies/custom headers may not work yet.
2. For a truly permanent laptop URL, reserve the phone's Wi-Fi IP in the router (DHCP reservation) or assign a stable IP. Otherwise the phone IP can change after reconnect/reboot.
3. Android may show a clipboard access notice when Auto Paste reads the clipboard. This is normal OS privacy behavior.
4. Cache is temporary app cache and can be cleared by Android under storage pressure.

## Build APK on GitHub (no PC required)

1. Upload this project to a GitHub repository.
2. Open **Actions** → **Build Android APK** → **Run workflow** (or push to `main`).
3. Download the artifact named `ExtremeStreamRelay-debug`.
4. Install `app-debug.apk` on the phone.

## Laptop / Energy Media Player

1. Phone and laptop must be on the same Wi-Fi/LAN.
2. Start relay in the Android app.
3. Copy the M3U URL displayed by the app.
4. Open it in Energy Media Player once.
5. For future movies, only change the source URL in the phone app.

## Cache behavior

Movie file size can be much larger than 2 GB. The app stores small 4 MB chunks. When the configured cache limit is reached, least-recently-used chunks are deleted so newer playback data can be cached.
