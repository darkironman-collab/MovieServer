package com.extreme.streamrelay;

import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.IBinder;
import android.os.PowerManager;

import androidx.annotation.Nullable;
import androidx.core.app.NotificationCompat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RelayService extends Service {
    public static final String ACTION_START_OR_UPDATE = "com.extreme.streamrelay.START_OR_UPDATE";
    private static final String CHANNEL_ID = "relay_channel";

    private RelayEngine engine;
    private RelayHttpServer server;
    private PowerManager.WakeLock wakeLock;
    private SharedPreferences prefs;
    private ExecutorService worker;

    @Override public void onCreate() {
        super.onCreate();
        prefs = getSharedPreferences("relay", MODE_PRIVATE);
        worker = Executors.newSingleThreadExecutor();
        createChannel();
        startForeground(7, buildNotification("Ready"));

        PowerManager pm = (PowerManager) getSystemService(POWER_SERVICE);
        if (pm != null) {
            wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "ExtremeStreamRelay::Relay");
            wakeLock.acquire();
        }
    }

    @Override public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null && ACTION_START_OR_UPDATE.equals(intent.getAction())) {
            final String url = intent.getStringExtra("url");
            final long cacheBytes = Math.min(
                    intent.getLongExtra("cacheBytes", 1024L * 1024L * 1024L),
                    2048L * 1024L * 1024L
            );
            final int threads = Math.max(2, Math.min(intent.getIntExtra("threads", 8), 16));

            prefs.edit().putString("status", "connecting...").apply();
            updateNotification("Connecting to source...");
            worker.submit(() -> startOrUpdateRelay(url, cacheBytes, threads));
        }
        return START_STICKY;
    }

    private void startOrUpdateRelay(String url, long cacheBytes, int threads) {
        try {
            if (engine == null) engine = new FastRelayEngine(this);
            engine.configure(url, cacheBytes, threads);

            if (server == null) {
                server = new RelayHttpServer(engine);
                server.start();
            }

            String format = engine.getStreamPath().contains(".")
                    ? engine.getStreamPath().substring(engine.getStreamPath().lastIndexOf('.') + 1).toUpperCase()
                    : "ORIGINAL";

            String mode = engine.isRangeSupported()
                    ? "Multi-thread Range"
                    : "Sequential fallback";

            String runningStatus = engine.isRangeSupported()
                    ? "running • multi-thread mode"
                    : "running • sequential fallback (source has no Range)";

            prefs.edit()
                    .putString("status", runningStatus)
                    .putLong("cacheMax", engine.getMaxCacheBytes())
                    .putString("streamPath", engine.getStreamPath())
                    .putString("contentType", engine.getContentType())
                    .putString("format", format)
                    .putString("relayMode", mode)
                    .putLong("sourceLength", engine.getSourceLength())
                    .putBoolean("rangeSupported", engine.isRangeSupported())
                    .apply();

            updateNotification("Relay active • " + format + " • " + mode);
        } catch (Throwable e) {
            String detail = describeError(e);
            prefs.edit().putString("status", "error: " + detail).apply();
            updateNotification("Relay error: " + shortText(detail, 55));
        }
    }

    private String describeError(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();

        String name = root.getClass().getSimpleName();
        if (name == null || name.trim().isEmpty()) name = root.getClass().getName();
        String msg = root.getMessage();
        if (msg == null || msg.trim().isEmpty()) return name;
        return name + ": " + msg.trim();
    }

    private String shortText(String s, int max) {
        if (s == null) return "Unknown error";
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }

    private void updateNotification(String text) {
        NotificationManager nm = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        if (nm != null) nm.notify(7, buildNotification(text));
    }

    @Override public void onDestroy() {
        if (worker != null) worker.shutdownNow();
        if (server != null) server.stop();
        if (engine != null) engine.close();
        if (wakeLock != null && wakeLock.isHeld()) wakeLock.release();
        prefs.edit().putString("status", "stopped").apply();
        super.onDestroy();
    }

    @Nullable @Override public IBinder onBind(Intent intent) { return null; }

    private void createChannel() {
        NotificationChannel channel = new NotificationChannel(
                CHANNEL_ID,
                "Extreme Stream Relay",
                NotificationManager.IMPORTANCE_LOW
        );
        NotificationManager nm = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        if (nm != null) nm.createNotificationChannel(channel);
    }

    private android.app.Notification buildNotification(String text) {
        Intent open = new Intent(this, MainActivity.class);
        PendingIntent pi = PendingIntent.getActivity(
                this,
                0,
                open,
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
        );
        return new NotificationCompat.Builder(this, CHANNEL_ID)
                .setSmallIcon(R.drawable.ic_relay_status)
                .setContentTitle("Extreme Stream Relay")
                .setContentText(text)
                .setOngoing(true)
                .setContentIntent(pi)
                .build();
    }
}
