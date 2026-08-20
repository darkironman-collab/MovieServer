package com.extreme.streamrelay;

import android.Manifest;
import android.content.ClipData;
import android.content.ClipboardManager;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.TextView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class MainActivity extends AppCompatActivity {
    private EditText urlInput;
    private CheckBox autoPaste, autoStart;
    private Spinner cacheSpinner, threadSpinner;
    private TextView statusText, sourceInfoText, directUrlText;
    private SharedPreferences prefs;
    private String lastClipboardUrl = "";
    private final Handler handler = new Handler(Looper.getMainLooper());

    private final Runnable statusRefresh = new Runnable() {
        @Override public void run() {
            refreshStatus();
            handler.postDelayed(this, 1000);
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        prefs = getSharedPreferences("relay", MODE_PRIVATE);
        urlInput = findViewById(R.id.urlInput);
        autoPaste = findViewById(R.id.autoPaste);
        autoStart = findViewById(R.id.autoStart);
        cacheSpinner = findViewById(R.id.cacheSpinner);
        threadSpinner = findViewById(R.id.threadSpinner);
        statusText = findViewById(R.id.statusText);
        sourceInfoText = findViewById(R.id.sourceInfoText);
        directUrlText = findViewById(R.id.directUrlText);

        Button pasteButton = findViewById(R.id.pasteButton);
        Button startButton = findViewById(R.id.startButton);
        Button stopButton = findViewById(R.id.stopButton);
        Button copyDirectButton = findViewById(R.id.copyDirectButton);

        List<String> cacheChoices = Arrays.asList("256 MB", "512 MB", "1 GB", "1.5 GB", "2 GB");
        cacheSpinner.setAdapter(new ArrayAdapter<>(this, android.R.layout.simple_spinner_dropdown_item, cacheChoices));

        List<String> threadChoices = Arrays.asList("2", "4", "8", "12", "16");
        threadSpinner.setAdapter(new ArrayAdapter<>(this, android.R.layout.simple_spinner_dropdown_item, threadChoices));

        urlInput.setText(prefs.getString("url", ""));
        autoPaste.setChecked(prefs.getBoolean("autoPaste", true));
        autoStart.setChecked(prefs.getBoolean("autoStart", false));
        cacheSpinner.setSelection(prefs.getInt("cacheIndex", 2));
        threadSpinner.setSelection(prefs.getInt("threadIndex", 2));

        pasteButton.setOnClickListener(v -> pasteFromClipboard(false));
        startButton.setOnClickListener(v -> startRelay());
        stopButton.setOnClickListener(v -> {
            stopService(new Intent(this, RelayService.class));
            prefs.edit().putString("status", "stopped").apply();
            refreshStatus();
        });
        copyDirectButton.setOnClickListener(v -> copyDirectUrl());

        if (Build.VERSION.SDK_INT >= 33 &&
                ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.POST_NOTIFICATIONS}, 100);
        }
    }

    @Override protected void onResume() {
        super.onResume();
        if (autoPaste.isChecked()) pasteFromClipboard(true);
        handler.removeCallbacks(statusRefresh);
        handler.post(statusRefresh);
    }

    @Override protected void onPause() {
        super.onPause();
        handler.removeCallbacks(statusRefresh);
        saveUiPrefs();
    }

    private void pasteFromClipboard(boolean auto) {
        ClipboardManager cm = (ClipboardManager) getSystemService(Context.CLIPBOARD_SERVICE);
        if (cm == null || !cm.hasPrimaryClip()) return;
        ClipData clip = cm.getPrimaryClip();
        if (clip == null || clip.getItemCount() == 0) return;
        CharSequence text = clip.getItemAt(0).coerceToText(this);
        if (text == null) return;

        String s = text.toString().trim();
        if (!isValidHttpUrl(s)) {
            if (!auto) Toast.makeText(this, "Clipboard does not contain a valid HTTP/HTTPS URL", Toast.LENGTH_SHORT).show();
            return;
        }

        if (auto && s.equals(lastClipboardUrl)) return;
        lastClipboardUrl = s;
        urlInput.setText(s);
        if (!auto) Toast.makeText(this, "Link pasted", Toast.LENGTH_SHORT).show();
        if (auto && autoStart.isChecked()) startRelay();
    }

    private boolean isValidHttpUrl(String s) {
        return s.startsWith("http://") || s.startsWith("https://");
    }

    private long selectedCacheBytes() {
        switch (cacheSpinner.getSelectedItemPosition()) {
            case 0: return 256L * 1024L * 1024L;
            case 1: return 512L * 1024L * 1024L;
            case 2: return 1024L * 1024L * 1024L;
            case 3: return 1536L * 1024L * 1024L;
            default: return 2048L * 1024L * 1024L;
        }
    }

    private int selectedThreads() {
        int[] values = {2, 4, 8, 12, 16};
        int index = threadSpinner.getSelectedItemPosition();
        if (index < 0 || index >= values.length) return 8;
        return values[index];
    }

    private void startRelay() {
        String url = urlInput.getText().toString().trim();
        if (!isValidHttpUrl(url)) {
            Toast.makeText(this, "Paste a valid HTTP/HTTPS media URL", Toast.LENGTH_LONG).show();
            return;
        }

        saveUiPrefs();
        prefs.edit()
                .putString("status", "starting...")
                .putString("streamPath", guessStreamPath(url))
                .apply();
        refreshStatus();

        Intent i = new Intent(this, RelayService.class);
        i.setAction(RelayService.ACTION_START_OR_UPDATE);
        i.putExtra("url", url);
        i.putExtra("cacheBytes", selectedCacheBytes());
        i.putExtra("threads", selectedThreads());
        ContextCompat.startForegroundService(this, i);
        Toast.makeText(this, "Connecting to source...", Toast.LENGTH_SHORT).show();
    }

    private void saveUiPrefs() {
        prefs.edit()
                .putString("url", urlInput.getText().toString().trim())
                .putBoolean("autoPaste", autoPaste.isChecked())
                .putBoolean("autoStart", autoStart.isChecked())
                .putInt("cacheIndex", cacheSpinner.getSelectedItemPosition())
                .putInt("threadIndex", threadSpinner.getSelectedItemPosition())
                .apply();
    }

    private void refreshStatus() {
        String ip = NetworkUtils.getLocalIpv4();
        String base = ip == null ? null : "http://" + ip + ":" + RelayHttpServer.PORT;
        String streamPath = prefs.getString("streamPath", "/stream");
        if (streamPath == null || streamPath.trim().isEmpty()) streamPath = "/stream";

        long used = prefs.getLong("cacheUsed", 0);
        long max = prefs.getLong("cacheMax", selectedCacheBytes());
        String state = prefs.getString("status", "stopped");
        statusText.setText("Status: " + state + "\nCache: " + humanBytes(used) + " / " + humanBytes(max));

        String format = prefs.getString("format", "");
        String contentType = prefs.getString("contentType", "application/octet-stream");
        long sourceLength = prefs.getLong("sourceLength", -1);
        boolean range = prefs.getBoolean("rangeSupported", false);
        String relayMode = prefs.getString("relayMode", range ? "Multi-thread Range" : "Sequential fallback");

        StringBuilder info = new StringBuilder("Source: ");
        if (format != null && !format.isEmpty()) info.append(format).append(" • ");
        if (sourceLength > 0) info.append(humanBytes(sourceLength)).append(" • ");
        info.append(contentType == null ? "original media" : contentType);

        if (state != null && state.startsWith("running")) {
            info.append("\nMode: ").append(relayMode);
            if (!range) info.append(" • large files supported • cache ≤ 2 GB");
        }
        sourceInfoText.setText(info.toString());

        if (base == null) {
            directUrlText.setText("Direct Stream URL: Connect phone to Wi-Fi");
        } else {
            directUrlText.setText("Direct Stream URL: " + base + streamPath);
        }
    }

    private void copyDirectUrl() {
        String ip = NetworkUtils.getLocalIpv4();
        if (ip == null) {
            Toast.makeText(this, "Phone is not connected to a LAN/Wi-Fi network", Toast.LENGTH_SHORT).show();
            return;
        }

        String path = prefs.getString("streamPath", "/stream");
        if (path == null || path.trim().isEmpty()) path = "/stream";
        String url = "http://" + ip + ":" + RelayHttpServer.PORT + path;

        ClipboardManager cm = (ClipboardManager) getSystemService(Context.CLIPBOARD_SERVICE);
        if (cm != null) cm.setPrimaryClip(ClipData.newPlainText("Extreme Stream Relay direct URL", url));
        Toast.makeText(this, "Direct stream URL copied", Toast.LENGTH_SHORT).show();
    }

    private String guessStreamPath(String url) {
        String lower = url.toLowerCase(Locale.ROOT);
        String noQuery = lower.split("\\?", 2)[0];
        String[] exts = {"mkv", "mp4", "m4v", "webm", "avi", "mov", "m2ts", "ts", "mpg", "mpeg"};
        for (String ext : exts) {
            if (noQuery.endsWith("." + ext)) return "/stream." + ext;
        }
        return "/stream";
    }

    private String humanBytes(long b) {
        if (b < 0) return "unknown";
        if (b >= 1024L * 1024L * 1024L) {
            return String.format(Locale.US, "%.2f GB", b / (1024.0 * 1024.0 * 1024.0));
        }
        if (b >= 1024L * 1024L) {
            return String.format(Locale.US, "%.0f MB", b / (1024.0 * 1024.0));
        }
        if (b >= 1024L) {
            return String.format(Locale.US, "%.0f KB", b / 1024.0);
        }
        return b + " B";
    }
}
