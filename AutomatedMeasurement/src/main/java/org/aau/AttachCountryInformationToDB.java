package org.aau;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpResponse;
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
import static org.aau.RipeAtlasMeasurement.apiKey;
import static org.aau.SQLiteConnector.attachCountryInformationToDB;
import static org.aau.SQLiteConnector.client;

public class AttachCountryInformationToDB {

    public static void main(String[] args) {
        //attachCountryInformationToDB();
        getRealTargetUrl();
    }

    public static void getRealTargetUrl() {

        ObjectMapper mapper = new ObjectMapper();

        HashMap<Integer, String> targetAddresses = SQLiteConnector.getTargetAddresses();
        HashMap<String, String> urlMap = new HashMap<>();
        for(int id : targetAddresses.keySet()) {
            String resultsUrl = "https://atlas.ripe.net/api/v2/measurements/" + id + "/";
            String dst_addr = targetAddresses.get(id);

            CloseableHttpClient client = HttpClients.createDefault();
            HttpGet get = new HttpGet(resultsUrl);
            get.setHeader("Authorization", "Key " + apiKey);

            TextNode result = null;
            try {
                result = client.execute(get, (ClassicHttpResponse response) -> {
                    String body = EntityUtils.toString(response.getEntity());
                    JsonNode root = mapper.readTree(body);
                    return (TextNode)root.get("target");
                });
            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println(result.asText().replaceAll("\"", ""));

            urlMap.put(dst_addr, result.asText().replaceAll("\"", ""));
        }

        SQLiteConnector.attachUrlToDB(urlMap);



        System.out.println("f");
    }

}
