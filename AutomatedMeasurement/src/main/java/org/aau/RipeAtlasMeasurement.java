package org.aau;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.tongfei.progressbar.ProgressBar;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.ContentType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.jline.utils.Log;


public class RipeAtlasMeasurement {

    //set RIPE_API_KEY=<Key> in environment beforehand!
    static String apiKey = System.getenv("RIPE_API_KEY");

    static String TARGET_URLS = "targets";

    static int CONCURRENT_MEASUREMENT_LIMIT = 100;
    static int CONCURRENT_MEASUREMENT_STARTER_THREADS = 50;


    static AtomicInteger activeMeasurements = new AtomicInteger(0);
    static AtomicInteger activeMeasurementThreads = new AtomicInteger(0);
    static AtomicInteger traversalIndex = new AtomicInteger(0);

    static HashMap<Integer, AtomicBoolean> targetsInActiveMeasurementMap = new HashMap<>();
    static ConcurrentHashMap<Long, Integer> measurementIdToMeasurementIndexMap = new ConcurrentHashMap<>();

    // Extracted with CountryCodeFetcher
    static String[] countryCodesWithProbes = new String[]{"AD", "AE", "AF", "AL", "AM", "AO", "AR", "AT", "AU", "AX", "AZ",
            "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BJ", "BL", "BN", "BO", "BR", "BT", "BW", "BY", "BZ", "CA", "CD", "CG",
            "CH", "CI", "CK", "CL", "CM", "CN", "CO", "CR", "CV", "CW", "CY", "CZ", "DE", "DJ", "DK", "DO", "DZ", "EC", "EE",
            "EG", "ES", "FI", "FJ", "FM", "FR", "GB", "GE", "GF", "GG", "GH", "GL", "GP", "GQ", "GR", "GT", "GU", "HK", "HN",
            "HR", "HT", "HU", "ID", "IE", "IL", "IM", "IN", "IQ", "IR", "IS", "IT", "JO", "JP", "KE", "KG", "KH", "KI", "KN",
            "KR", "KW", "KY", "KZ", "LA", "LB", "LC", "LI", "LK", "LS", "LT", "LU", "LV", "MA", "MC", "MD", "ME", "MG", "MH",
            "MK", "ML", "MM", "MN", "MP", "MR", "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA", "NC", "NG", "NI", "NL", "NO",
            "NP", "NZ", "OM", "PA", "PE", "PF", "PG", "PH", "PK", "PL", "PR", "PS", "PT", "PW", "PY", "QA", "RE", "RO", "RS",
            "RU", "RW", "SA", "SC", "SE", "SG", "SI", "SK", "SL", "SN", "SV", "TD", "TH", "TJ", "TL", "TN", "TO", "TR", "TT",
            "TW", "TZ", "UA", "UG", "US", "UY", "UZ", "VA", "VE", "VI", "VN", "XK", "YE", "ZA", "ZM", "ZW"};

    static int BATCH_SIZE = Math.min(25, countryCodesWithProbes.length);

    static List<Map<String, Object>> targets = new ArrayList<>();

    // Ideally we would use List<int[]>, but that would make the assertMeasurementIndicesConstraint method more complicated
    static List<List<Integer>> traversalOrder = new ArrayList<>();

    static {
        Log.info("Reading Target URLs");
        try {
            BufferedReader br = new BufferedReader(new FileReader(TARGET_URLS));

            String line;

            for (int i = 0; (line = br.readLine()) != null; i++) {
                if (line.startsWith("NOT_REACHABLE")) {
                    continue;
                }
                Map<String, Object> definition = new HashMap<>();
                definition.put("target", line);
                definition.put("description", "Rank " + i);
                definition.put("type", "ping");
                definition.put("af", 4);
                definition.put("is_oneoff", true);

                targets.add(definition);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < targets.size(); i++) {
            targetsInActiveMeasurementMap.put(i, new AtomicBoolean(false));
        }

        Log.info("Computing optimal request order");
        traversalOrder = getMeasurementIndices();

        Log.info("Asserting wholeness and uniqueness of measurements in the traversal order");
        assertMeasurementIndicesConstraint();
        Log.info("Traversal order is valid");
    }

    public static Map<String, Object> generateDefinitionPayload(int startCountryIndex, int targetIndex) {
        Map<String, Object> payload = new HashMap<>();
        List<Map<String, Object>> probeList = new ArrayList<>();

        for (int i = startCountryIndex; i < Math.min(countryCodesWithProbes.length, startCountryIndex + BATCH_SIZE); i++) {

            Map<String, Object> probe = new HashMap<>();
            probe.put("requested", 1);
            probe.put("type", "country");
            probe.put("value", countryCodesWithProbes[i]);

            probeList.add(probe);
        }

        payload.put("definitions", List.of(targets.get(targetIndex)));
        payload.put("probes", probeList);

        Log.info("Generated payload for target " + targetIndex + " and countries from index " + startCountryIndex + " until " + Math.min(countryCodesWithProbes.length, startCountryIndex + BATCH_SIZE));

        return payload;
    }


    public static List<Long> createMeasurements(int measurementIndex) {
        String apiKey = System.getenv("RIPE_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("RIPE_API_KEY environment variable not set.");
        }

        String url = "https://atlas.ripe.net/api/v2/measurements/";

        int targetIndex = traversalOrder.get(measurementIndex).get(0);
        int countryCodeIndex = traversalOrder.get(measurementIndex).get(1);

        Map<String, Object> payload = generateDefinitionPayload(countryCodeIndex, targetIndex);

        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

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
                    Log.error("Failed to create measurement. Target: " + targets.get(targetIndex).get("target") + " Country: " + countryCodesWithProbes[countryCodeIndex] + " Status " + status + ": " + responseBody);
                    activeMeasurements.decrementAndGet();
                    return new ArrayList<>();
                }

                JsonNode root = mapper.readTree(responseBody);
                List<Long> measurementIds = new ArrayList<>();
                JsonNode measurements = root.path("measurements");
                for (int i = 0; i < measurements.size(); i++) {
                    long id = measurements.get(i).asLong();

                    for (int j = countryCodeIndex; j < Math.min(countryCodesWithProbes.length, countryCodeIndex + BATCH_SIZE); j++) {
                        //TODO Fix this by dynamically accessing the ripe atlass api (now its lots of duplicates)
                        SQLiteConnector.createMeasurementContext(id, (String) targets.get(targetIndex).get("target"), countryCodesWithProbes[j]);

                    }
                    measurementIds.add(id);
                    measurementIdToMeasurementIndexMap.put(id, measurementIndex);
                }

                return measurementIds;
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getMeasurementStatus(CloseableHttpClient client, long measurementId) throws Exception {
        String statusUrl = "https://atlas.ripe.net/api/v2/measurements/" + measurementId + "/";

        HttpGet statusGet = new HttpGet(statusUrl);
        statusGet.setHeader("Authorization", "Key " + apiKey);

        return client.execute(statusGet, (ClassicHttpResponse response) -> {
            int code = response.getCode();
            if (code != 200) {
                Log.error("Failed to fetch measurement status: HTTP " + code);
            }

            String body = EntityUtils.toString(response.getEntity());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(body);
            JsonNode statusNode = root.path("status").path("name");

            if (statusNode.isMissingNode()) {
                Log.error("Measurement status not found in response.");
            }

            return statusNode.asText();
        });
    }


    public static void waitForResultAndSaveToDb(long measurementId, int timeoutIntMs, int intervalInMs, ProgressBar pb) throws Exception {
        String resultsUrl = "https://atlas.ripe.net/api/v2/measurements/" + measurementId + "/results/";
        ObjectMapper mapper = new ObjectMapper();

        int elapsed = 0;

        CloseableHttpClient client = HttpClients.createDefault();
        AtomicBoolean targetIsInMeasurement = targetsInActiveMeasurementMap.get(traversalOrder.get(measurementIdToMeasurementIndexMap.get(measurementId)).get(0));

        while (elapsed < timeoutIntMs) {

            String status = getMeasurementStatus(client, measurementId);
            if (status.equalsIgnoreCase("Failed") || status.equalsIgnoreCase("No suitable probes")) {
                SQLiteConnector.saveFailedMeasurement(measurementId, "MEASUREMENT", status);
                Log.info("Measurement " + measurementId + " failed.");
                activeMeasurements.decrementAndGet();
                targetIsInMeasurement.set(false);
                pb.step();
                return;
                //TODO: Sometimes the status seems to stay a few minutes at ongoing then goes to Stopped, maybe already consume ongoing messages if results are present
            } else if (status.equalsIgnoreCase("Stopped")) {
                HttpGet get = new HttpGet(resultsUrl);
                get.setHeader("Authorization", "Key " + apiKey);

                String result = client.execute(get, (ClassicHttpResponse response) -> {
                    String body = EntityUtils.toString(response.getEntity());
                    JsonNode root = mapper.readTree(body);
                    if (root.isArray() && root.size() > 0) {
                        try {
                            //TODO: Not all requested probes are actually approved. Add the missing probes to a buffer to retry
                            SQLiteConnector.saveResults(body);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        pb.step();
                        activeMeasurements.decrementAndGet();
                        targetIsInMeasurement.set(false);
                        return body;
                    }
                    return null;
                });

                if (result != null) {
                    return;
                }
            }

            Thread.sleep(intervalInMs);
            elapsed += intervalInMs;
        }

        Log.info("Measurement " + measurementId + " timed out, added to pending measurements for future retrieval");
        SQLiteConnector.savePendingMeasurement(measurementId);
        pb.step();
    }

    public static Thread startAsynchronousMeasurementThread(ProgressBar pb, ConcurrentLinkedQueue<Long> measurementIds) {
        activeMeasurementThreads.incrementAndGet();
        return Thread.startVirtualThread(() -> {
            Log.info("Thread " + Thread.currentThread().getName() + " started");

            while (true) {
                int measurementIndex = traversalIndex.getAndUpdate((t) -> {
                    if (t < traversalOrder.size()) {
                        return t + 1;
                    }
                    return t;
                });
                if (measurementIndex >= traversalOrder.size()) {
                    break;
                }

                Log.info("Thread " + Thread.currentThread().getName() + " aquired measurement index " + measurementIndex);

                while (true) {
                    int current = activeMeasurements.get();

                    if (current >= CONCURRENT_MEASUREMENT_LIMIT) {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        continue;
                    }

                    if (activeMeasurements.compareAndSet(current, current + 1)) {
                        break;
                    }
                }

                AtomicBoolean targetIsInMeasurement = targetsInActiveMeasurementMap.get(traversalOrder.get(measurementIndex).get(0));


                while (true) {
                    if (targetIsInMeasurement.get()) {
                        Log.info("Target of measurement " + measurementIndex + " is already in an active measurement, waiting for completion of measurement...");
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        continue;
                    }
                    if (targetIsInMeasurement.compareAndSet(false, true)) {
                        break;
                    }
                }

                measurementIds.addAll(createMeasurements(measurementIndex));
                pb.step();
            }

            activeMeasurementThreads.decrementAndGet();
        });
    }

    public static List<List<Integer>> getMeasurementIndices() {
        int cols = countryCodesWithProbes.length;
        int rows = targets.size();
        int batchesInRow = Math.ceilDiv(cols, BATCH_SIZE);

        List<List<Integer>> measurementIndices = new ArrayList<>();

        int batchCounter = 0;
        int rowIterations = 0;

        for (int i = 0; i < rows * batchesInRow; i++) {
            if (i != 0 && i % rows == 0) {
                rowIterations++;
            }
            if (batchCounter == (batchesInRow + rowIterations) || i % rows == 0) {
                batchCounter = rowIterations;
            }

            List<Integer> measurementIndex = new ArrayList<>();
            measurementIndex.add(i % rows);
            measurementIndex.add((batchCounter % batchesInRow) * BATCH_SIZE);

            //Log.info(i % rows + " " + ((batchCounter % batchesInRow) * BATCH_SIZE));
            measurementIndices.add(measurementIndex);
            batchCounter++;

        }

        return measurementIndices;
    }

    public static void assertMeasurementIndicesConstraint() {
        int cols = countryCodesWithProbes.length;
        int rows = targets.size();

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j += BATCH_SIZE) {
                List<Integer> testIndices = new ArrayList<>();
                testIndices.add(i);
                testIndices.add(j);
                //Log.info(i + " " + j);

                if (!traversalOrder.contains(testIndices)) {
                    System.err.println("Detected violated measurement index constraints! Measurement indices do not contain " + i + " " + j * BATCH_SIZE);
                    System.exit(1);
                }

                if (traversalOrder.indexOf(testIndices) != traversalOrder.lastIndexOf(testIndices)) {
                    System.err.println("Detected violated measurement index constraints! Measurement index is duplicated " + i + " " + j * BATCH_SIZE);
                    System.exit(1);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Log.info("Starting Ripe Atlas measurement");
        List<Thread> startingThreads = new ArrayList<>();

        ConcurrentLinkedQueue<Long> measurementIds = new ConcurrentLinkedQueue<>();

        try (ProgressBar startPb = new ProgressBar("Sending measurement requests", traversalOrder.size());
             ProgressBar pb = new ProgressBar("Receiving measurement results", measurementIds.size())) {

            for (int i = 0; i < CONCURRENT_MEASUREMENT_STARTER_THREADS; i++) {
                startingThreads.add(startAsynchronousMeasurementThread(startPb, measurementIds));
            }

            List<Thread> threads = new ArrayList<>();
            outerloop:
            while (activeMeasurementThreads.get() > 0 || !measurementIds.isEmpty()) {
                while (measurementIds.peek() == null) {
                    if (activeMeasurementThreads.get() == 0 && measurementIds.isEmpty()) {
                        break outerloop;
                    }
                    Thread.sleep(200);
                }
                long id = measurementIds.poll();
                Thread thread = Thread.startVirtualThread(() -> {
                    try {
                        waitForResultAndSaveToDb(id, 600000, 2000, pb);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                threads.add(thread);
            }

            for (Thread thread : startingThreads) {
                thread.join();
            }

            for (Thread thread : threads) {
                thread.join();
            }
        }
    }
}
