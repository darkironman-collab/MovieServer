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

public class RelayService extends Service {
    public static final String ACTION_START_OR_UPDATE = "com.extreme.streamrelay.START_OR_UPDATE";
    private static final String CHANNEL_ID = "relay_channel";
    private RelayEngine engine;
    private RelayHttpServer server;
    private PowerManager.WakeLock wakeLock;
    private SharedPreferences prefs;

    @Override public void onCreate() {
        super.onCreate();
        prefs = getSharedPreferences("relay", MODE_PRIVATE);
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
            String url = intent.getStringExtra("url");
            long cacheBytes = Math.min(intent.getLongExtra("cacheBytes", 1024L * 1024L * 1024L), 2048L * 1024L * 1024L);
            int threads = Math.max(2, Math.min(intent.getIntExtra("threads", 8), 16));
            try {
                if (engine == null) engine = new RelayEngine(this);
                engine.configure(url, cacheBytes, threads);
                if (server == null) {
                    server = new RelayHttpServer(engine);
                    server.start();
                }
                prefs.edit().putString("status", "running on port " + RelayHttpServer.PORT).putLong("cacheMax", cacheBytes).apply();
                NotificationManager nm = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
                if (nm != null) nm.notify(7, buildNotification("Streaming relay active"));
            } catch (Exception e) {
                prefs.edit().putString("status", "error: " + e.getMessage()).apply();
            }
        }
        return START_STICKY;
    }

    @Override public void onDestroy() {
        if (server != null) server.stop();
        if (engine != null) engine.close();
        if (wakeLock != null && wakeLock.isHeld()) wakeLock.release();
        prefs.edit().putString("status", "stopped").apply();
        super.onDestroy();
    }

    @Nullable @Override public IBinder onBind(Intent intent) { return null; }

    private void createChannel() {
        NotificationChannel channel = new NotificationChannel(CHANNEL_ID, "Extreme Stream Relay", NotificationManager.IMPORTANCE_LOW);
        NotificationManager nm = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        if (nm != null) nm.createNotificationChannel(channel);
    }

    private android.app.Notification buildNotification(String text) {
        Intent open = new Intent(this, MainActivity.class);
        PendingIntent pi = PendingIntent.getActivity(this, 0, open, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        return new NotificationCompat.Builder(this, CHANNEL_ID)
                .setSmallIcon(android.R.drawable.stat_sys_download_done)
                .setContentTitle("Extreme Stream Relay")
                .setContentText(text)
                .setOngoing(true)
                .setContentIntent(pi)
                .build();
    }
}
