package org.aau;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.ContentType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.ClassicHttpResponse;

public class RipeAtlasMeasurement {

    //set RIPE_API_KEY=<Key> in environment beforehand!
    static String apiKey = System.getenv("RIPE_API_KEY");

    static String RIPE_URL = "https://atlas.ripe.net/api/v2/measurements";

    public static long createMeasurement() throws Exception {
        String apiKey = System.getenv("RIPE_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("RIPE_API_KEY environment variable not set.");
        }

        String url = "https://atlas.ripe.net/api/v2/measurements/";

        // Build JSON payload
        Map<String, Object> definition = new HashMap<>();
        definition.put("target", "8.8.8.8");
        definition.put("description", "Ping Google DNS");
        definition.put("type", "ping");
        definition.put("af", 4);
        definition.put("is_oneoff", true);

        Map<String, Object> probe = new HashMap<>();
        probe.put("requested", 3);
        probe.put("type", "area");
        probe.put("value", "WW");

        Map<String, Object> payload = new HashMap<>();
        payload.put("definitions", List.of(definition));
        payload.put("probes", List.of(probe));

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(payload);

        // Send POST request
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(url);
            post.setHeader("Authorization", "Key " + apiKey);
            post.setHeader("Content-Type", "application/json");
            post.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));

            return client.execute(post, (ClassicHttpResponse response) -> {
                int status = response.getCode();
                String responseBody = EntityUtils.toString(response.getEntity());
                if (status != 201) {
                    throw new RuntimeException("Failed to create measurement. Status " + status + ": " + responseBody);
                }

                JsonNode root = mapper.readTree(responseBody);
                long measurementId = root.path("measurements").get(0).asLong();
                System.out.println("Measurement ID: " + measurementId);
                return measurementId;
            });
        }
    }


    public static String waitForResults(long measurementId, int timeoutSeconds) throws Exception {
        String resultsUrl = "https://atlas.ripe.net/api/v2/measurements/" + measurementId + "/results/";
        ObjectMapper mapper = new ObjectMapper();

        int elapsed = 0;
        int interval = 5;

        CloseableHttpClient client = HttpClients.createDefault();
        while (elapsed < timeoutSeconds) {
            HttpGet get = new HttpGet(resultsUrl);
            get.setHeader("Authorization", "Key " + apiKey);

            String result = client.execute(get, (ClassicHttpResponse response) -> {
                String body = EntityUtils.toString(response.getEntity());
                JsonNode root = mapper.readTree(body);
                if (root.isArray() && root.size() > 0) {
                    return body;
                }
                return null;
            });

            if (result != null) {
                return result;
            }

            System.out.println("Waiting for results...");
            Thread.sleep(interval * 1000);
            elapsed += interval;
        }

        throw new RuntimeException("Timed out waiting for measurement results.");

    }


    public static void main(String[] args) throws Exception {
        long measurementId = createMeasurement();

        String result = waitForResults(measurementId, 60);

        System.out.println(result);
    }
}
