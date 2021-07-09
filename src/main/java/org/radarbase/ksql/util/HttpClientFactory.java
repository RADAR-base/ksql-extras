package org.radarbase.ksql.util;

import java.time.Duration;
import okhttp3.OkHttpClient;

public class HttpClientFactory {

    private static OkHttpClient _INSTANCE;

    private HttpClientFactory() {

    }

    public static OkHttpClient getClient() {
        if (_INSTANCE==null) {
            _INSTANCE = new OkHttpClient.Builder()
                    .callTimeout(Duration.ofSeconds(30))
                    .readTimeout(Duration.ofSeconds(30))
                    .connectTimeout(Duration.ofSeconds(20))
                    .build();
        }
        return _INSTANCE;
    }
}
