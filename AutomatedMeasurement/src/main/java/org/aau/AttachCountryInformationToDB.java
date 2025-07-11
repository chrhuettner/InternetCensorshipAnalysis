package org.aau;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.jline.utils.Log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.aau.RipeAtlasMeasurement.TARGET_URLS;
import static org.aau.SQLiteConnector.attachCountryInformationToDB;

public class AttachCountryInformationToDB {

    public static void main(String[] args) {
        addTargetUrlToMeasurementResults();
        attachCountryInformationToDB();

    }

    public static void addTargetUrlToMeasurementResults() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(TARGET_URLS));

            String line;

            HashMap<String, String> urlMap = new HashMap<>();
            while ((line = br.readLine()) != null) {
                if (line.startsWith("NOT_REACHABLE")) {
                    continue;
                }
                InetAddress address = InetAddress.getByName(line);
                urlMap.put(address.getHostAddress(), line);

            }

            SQLiteConnector.attachUrlToDB(urlMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
