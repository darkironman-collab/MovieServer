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

public class MainActivity extends AppCompatActivity {
    private EditText urlInput;
    private CheckBox autoPaste, autoStart;
    private Spinner cacheSpinner, threadSpinner;
    private TextView statusText, m3uUrlText, playUrlText;
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
        m3uUrlText = findViewById(R.id.m3uUrlText);
        playUrlText = findViewById(R.id.playUrlText);
        Button pasteButton = findViewById(R.id.pasteButton);
        Button startButton = findViewById(R.id.startButton);
        Button stopButton = findViewById(R.id.stopButton);
        Button copyM3uButton = findViewById(R.id.copyM3uButton);

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
        copyM3uButton.setOnClickListener(v -> copyM3uUrl());

        if (Build.VERSION.SDK_INT >= 33 && ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.POST_NOTIFICATIONS}, 100);
        }
    }

    @Override protected void onResume() {
        super.onResume();
        if (autoPaste.isChecked()) pasteFromClipboard(true);
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
        return values[threadSpinner.getSelectedItemPosition()];
    }

    private void startRelay() {
        String url = urlInput.getText().toString().trim();
        if (!isValidHttpUrl(url)) {
            Toast.makeText(this, "Paste a valid HTTP/HTTPS media URL", Toast.LENGTH_LONG).show();
            return;
        }
        saveUiPrefs();
        Intent i = new Intent(this, RelayService.class);
        i.setAction(RelayService.ACTION_START_OR_UPDATE);
        i.putExtra("url", url);
        i.putExtra("cacheBytes", selectedCacheBytes());
        i.putExtra("threads", selectedThreads());
        ContextCompat.startForegroundService(this, i);
        Toast.makeText(this, "Relay started/updated", Toast.LENGTH_SHORT).show();
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
        statusText.setText("Status: " + prefs.getString("status", "stopped") + "\nCache: " + humanBytes(prefs.getLong("cacheUsed", 0)) + " / " + humanBytes(prefs.getLong("cacheMax", selectedCacheBytes())));
        m3uUrlText.setText("M3U URL: " + (base == null ? "Connect phone to Wi-Fi" : base + "/permanent.m3u"));
        playUrlText.setText("Play URL: " + (base == null ? "--" : base + "/play"));
    }

    private void copyM3uUrl() {
        String ip = NetworkUtils.getLocalIpv4();
        if (ip == null) {
            Toast.makeText(this, "Phone is not connected to a LAN/Wi-Fi network", Toast.LENGTH_SHORT).show();
            return;
        }
        String url = "http://" + ip + ":" + RelayHttpServer.PORT + "/permanent.m3u";
        ClipboardManager cm = (ClipboardManager) getSystemService(Context.CLIPBOARD_SERVICE);
        if (cm != null) cm.setPrimaryClip(ClipData.newPlainText("Extreme Stream Relay M3U", url));
        Toast.makeText(this, "M3U URL copied", Toast.LENGTH_SHORT).show();
    }

    private String humanBytes(long b) {
        if (b >= 1024L * 1024L * 1024L) return String.format("%.2f GB", b / (1024.0 * 1024.0 * 1024.0));
        if (b >= 1024L * 1024L) return String.format("%.0f MB", b / (1024.0 * 1024.0));
        return b + " B";
    }
}
