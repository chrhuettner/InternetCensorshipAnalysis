package org.aau;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.jline.utils.Log;

import static org.aau.RipeAtlasMeasurement.apiKey;

public class SQLiteConnector {
    private static final String DB_URL = "jdbc:sqlite:ripeatlas.db";

    static PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();

    static CloseableHttpClient client;

    static {
        cm.setMaxTotal(100); // max total connections
        cm.setDefaultMaxPerRoute(20); // per target host

        client = HttpClients.custom().setConnectionManager(cm).build();
    }

    public static void main(String[] args) {

    }

    private static Connection c = null;

    public static Connection getConn() {
        if (c == null) {
            try {
                c = DriverManager.getConnection(DB_URL);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        return c;
    }

    static {
        Connection conn = getConn();
        try (Statement stmt = conn.createStatement()) {
            String sql = """
                    
                            CREATE TABLE IF NOT EXISTS measurement_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fw INTEGER,
                        mver TEXT,
                        lts INTEGER,
                        dst_name TEXT,
                        af INTEGER,
                        dst_addr TEXT,
                        src_addr TEXT,
                        proto TEXT,
                        ttl INTEGER,
                        size INTEGER,
                        dup INTEGER,
                        rcvd INTEGER,
                        sent INTEGER,
                        min REAL,
                        max REAL,
                        avg REAL,
                        msm_id INTEGER,
                        prb_id INTEGER,
                        timestamp INTEGER,
                        msm_name TEXT,
                        from_addr TEXT,
                        type TEXT,
                        group_id INTEGER,
                        step TEXT,
                        stored_timestamp INTEGER,
                        prb_country TEXT
                    )
                    """;

            String pending_Measurements_sql = """
                    CREATE TABLE IF NOT EXISTS pending_measurements (
                                           id LONG PRIMARY KEY
                                           );
                    """;

            String failed_Measurements_sql = """
                    CREATE TABLE IF NOT EXISTS failed_measurements (
                                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                                           failed_At TEXT,
                                           msm_id INTEGER,
                                           status TEXT
                                           );
                    """;

            String measurement_context_sql = """
                    CREATE TABLE IF NOT EXISTS measurement_context (
                                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                                           msm_id INTEGER,
                                           target TEXT,
                                           country_code TEXT
                                           );
                    """;


            stmt.execute(sql);
            stmt.execute(pending_Measurements_sql);
            stmt.execute(failed_Measurements_sql);
            stmt.execute(measurement_context_sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void attachUrlToDB(HashMap<String, String> urlMap) {
        Connection conn = getConn();
        try {

            for (String dst_addr : urlMap.keySet()) {
                String updateSQL = "UPDATE measurement_results SET dst_name = ? WHERE dst_addr = ?";
                String url = urlMap.get(dst_addr);
                PreparedStatement updateStmt = conn.prepareStatement(updateSQL);
                updateStmt.setString(1, url);
                updateStmt.setString(2, dst_addr);
                updateStmt.executeUpdate();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static HashMap<Integer, String> getTargetAddresses() {
        HashMap<Integer, String> targetAddresses = new HashMap<>();
        Connection conn = getConn();
        String selectSQL = "SELECT msm_id, dst_addr FROM measurement_results group by dst_addr";
        PreparedStatement selectStmt = null;
        try {
            selectStmt = conn.prepareStatement(selectSQL);

            ResultSet rs = selectStmt.executeQuery();


            while (rs.next()) {
                int id = rs.getInt("msm_id");
                String dst_addr = rs.getString("dst_addr");

                targetAddresses.put(id, dst_addr);

            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return targetAddresses;
    }

    public static void attachCountryInformationToDB() {
        Connection conn = getConn();
        String selectSQL = "SELECT id, prb_id FROM measurement_results WHERE prb_country IS NULL";
        PreparedStatement selectStmt = null;
        try {
            selectStmt = conn.prepareStatement(selectSQL);

            ResultSet rs = selectStmt.executeQuery();

            String updateSQL = "UPDATE measurement_results SET prb_country = ? WHERE id = ?";
            ConcurrentLinkedQueue<int[]> probeIdMap = new ConcurrentLinkedQueue<>();

            while (rs.next()) {
                int id = rs.getInt("id");
                int prbId = rs.getInt("prb_id");

                probeIdMap.add(new int[]{id, prbId});
            }

            List<Thread> threads = new ArrayList<>();

            AtomicInteger count = new AtomicInteger(0);

            for (int i = 0; i < 50; i++) {
                Thread thread = Thread.startVirtualThread(() -> {
                    while (true) {
                        int[] pair = probeIdMap.poll();
                        if (pair == null) {
                            return;
                        }
                        try {
                            PreparedStatement updateStmt = conn.prepareStatement(updateSQL);
                            String country = getCountryForProbe(pair[1]);
                            if (country != null) {
                                updateStmt.setString(1, country);
                                updateStmt.setInt(2, pair[0]);
                                updateStmt.executeUpdate();
                            }
                            count.incrementAndGet();

                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }

                });
                threads.add(thread);
            }

            Thread.startVirtualThread(() -> {
                while (probeIdMap.peek() != null) {
                    Log.info("Processed " + count.get() + " measurements");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            for (Thread thread : threads) {
                thread.join();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getCountryForProbe(int prbId) {
        String url = "https://atlas.ripe.net/api/v2/probes/" + prbId + "/";
        ObjectMapper mapper = new ObjectMapper();
        try {
            HttpGet request = new HttpGet(url);


            request.setHeader("Authorization", "Key " + apiKey);

            return client.execute(request, response -> {
                int status = response.getCode();
                if (status >= 200 && status < 300) {
                    String jsonString = EntityUtils.toString(response.getEntity());
                    JsonNode root = mapper.readTree(jsonString);
                    JsonNode countryCodeNode = root.get("country_code");
                    return countryCodeNode != null ? countryCodeNode.asText() : null;

                } else {
                    System.err.println("Failed to fetch probe " + prbId + " - HTTP status " + status);
                    return null;
                }
            });

        } catch (Exception e) {
            System.err.println("Error fetching country for probe " + prbId + ": " + e.getMessage());
            return null;
        }
    }

    public static void savePendingMeasurement(long id) {
        Connection conn = getConn();
        String insertSql = """
                    INSERT INTO pending_measurements (
                        id
                    ) VALUES (?)
                """;

        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {

            pstmt.setLong(1, id);

            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public static void deletePendingMeasurement(long id) {
        Connection conn = getConn();

        String insertSql = """
                    delete from pending_measurements where id = ?;
                """;

        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {

            pstmt.setLong(1, id);

            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void createMeasurementContext(long msm_id, String target, String countryCode) {
        Connection conn = getConn();
        String insertSql = """
                    insert into measurement_context (msm_id, target, country_code) values (?, ?, ?);
                """;

        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {

            pstmt.setLong(1, msm_id);
            pstmt.setString(2, target);
            pstmt.setString(3, countryCode);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public static List<Long> getPendingMeasurements() {
        List<Long> ids = new ArrayList<>();

        Connection conn = getConn();

        String insertSql = """
                  select * from pending_measurements;
                """;

        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {

            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                ids.add(resultSet.getLong("id"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return ids;
    }

    public static void saveFailedMeasurement(long msm_id, String failedAt, String status) {

        Connection conn = getConn();
        String insertSql = """
                    INSERT INTO failed_measurements (
                       msm_id, status, failed_At
                    ) VALUES (?, ? , ?)
                """;

        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {

            pstmt.setLong(1, msm_id);
            pstmt.setString(2, status);
            pstmt.setString(3, failedAt);


            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public static void saveResults(String jsonResults) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode resultsArray = mapper.readTree(jsonResults);

        Connection conn = getConn();
        String insertSql = """
                    INSERT INTO measurement_results (
                        fw, mver, lts, dst_name, af, dst_addr, src_addr, proto, ttl, size,
                        dup, rcvd, sent, min, max, avg, msm_id, prb_id, timestamp, msm_name,
                        from_addr, type, group_id, step, stored_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            for (JsonNode obj : resultsArray) {
                pstmt.setInt(1, obj.path("fw").asInt());
                pstmt.setString(2, obj.path("mver").asText());
                pstmt.setInt(3, obj.path("lts").asInt());
                pstmt.setString(4, obj.path("dst_name").asText());
                pstmt.setInt(5, obj.path("af").asInt());
                pstmt.setString(6, obj.path("dst_addr").asText());
                pstmt.setString(7, obj.path("src_addr").asText());
                pstmt.setString(8, obj.path("proto").asText());
                pstmt.setInt(9, obj.path("ttl").asInt());
                pstmt.setInt(10, obj.path("size").asInt());
                pstmt.setInt(11, obj.path("dup").asInt());
                pstmt.setInt(12, obj.path("rcvd").asInt());
                pstmt.setInt(13, obj.path("sent").asInt());
                pstmt.setDouble(14, obj.path("min").asDouble());
                pstmt.setDouble(15, obj.path("max").asDouble());
                pstmt.setDouble(16, obj.path("avg").asDouble());
                pstmt.setLong(17, obj.path("msm_id").asLong());
                pstmt.setInt(18, obj.path("prb_id").asInt());
                pstmt.setLong(19, obj.path("timestamp").asLong());
                pstmt.setString(20, obj.path("msm_name").asText());
                pstmt.setString(21, obj.path("from").asText());  // `from` is a reserved word, renamed to `from_addr`
                pstmt.setString(22, obj.path("type").asText());
                if (obj.path("group_id").isNull()) {
                    pstmt.setNull(23, Types.INTEGER);
                } else {
                    pstmt.setLong(23, obj.path("group_id").asLong());
                }
                if (obj.path("step").isNull()) {
                    pstmt.setNull(24, Types.VARCHAR);
                } else {
                    pstmt.setString(24, obj.path("step").asText());
                }
                pstmt.setLong(25, obj.path("stored_timestamp").asLong());

                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }
}

