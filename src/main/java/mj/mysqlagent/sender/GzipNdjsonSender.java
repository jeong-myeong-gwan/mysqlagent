package mj.mysqlagent.sender;

import okhttp3.*;

import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPOutputStream;

public class GzipNdjsonSender {
    private final OkHttpClient client;
    private final String url;
    private final String bearer;

    public GzipNdjsonSender(String url, String bearer) {
        this.url = url;
        this.bearer = bearer;
        this.client = new OkHttpClient.Builder()
                .retryOnConnectionFailure(true)
                .build();
    }

    public boolean sendGzip(byte[] ndjsonBytes) {
        try {
            byte[] gz = gzip(ndjsonBytes);

            RequestBody body = RequestBody.create(
                    gz,
                    MediaType.parse("application/x-ndjson")
            );

            Request req = new Request.Builder()
                    .url(url)
                    .addHeader("Authorization", "Bearer " + bearer)
                    .addHeader("Content-Encoding", "gzip")
                    .addHeader("Content-Type", "application/x-ndjson")
                    .post(body)
                    .build();

            try (Response resp = client.newCall(req).execute()) {
                return resp.isSuccessful();
            }
        } catch (Exception e) {
            return false;
        }
    }

    private byte[] gzip(byte[] raw) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
            gos.write(raw);
        }
        return baos.toByteArray();
    }
}