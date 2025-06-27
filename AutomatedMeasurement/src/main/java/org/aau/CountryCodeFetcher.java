package org.aau;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.util.*;
import java.util.stream.Collectors;

public class CountryCodeFetcher {

    public static Set<String> getConnectedCountryCodes() throws Exception {
        Set<String> countryCodes = new HashSet<>();
        ObjectMapper mapper = new ObjectMapper();
        String url = "https://atlas.ripe.net/api/v2/probes/?status_name=Connected&page_size=500";

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            while (url != null) {
                HttpGet get = new HttpGet(url);
                String response = client.execute(get, (ClassicHttpResponse httpResp) ->
                        EntityUtils.toString(httpResp.getEntity())
                );

                JsonNode root = mapper.readTree(response);
                for (JsonNode probe : root.path("results")) {
                    String cc = probe.path("country_code").asText();
                    if (!cc.isEmpty()) {
                        countryCodes.add(cc);
                    }
                }

                JsonNode next = root.path("next");
                url = next.isNull() ? null : next.asText();
            }
        }

        return countryCodes;
    }

    public static void main(String[] args) throws Exception {
        Set<String> codes = getConnectedCountryCodes();
        List<String> sorted = codes.stream().sorted().collect(Collectors.toList());
        System.out.println("Country codes with connected probes:");
        sorted.forEach(System.out::println);
    }
}
