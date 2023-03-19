package com.example.smaato;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

@SpringBootApplication
@EnableScheduling
@Controller
public class RestServiceApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestServiceApplication.class);

    private final ConcurrentHashMap<Integer, LongAdder> uniqueRequests = new ConcurrentHashMap<>();
    private final ConnectionPool connectionPool = new ConnectionPool();//new ConnectionPool(200, 5 * 60 * 1000); // 200 max idle connections, 5 minute timeout
    private final OkHttpClient httpClient = new OkHttpClient.Builder().connectionPool(connectionPool).build();

    @Value("${endpoint:}")
    private String endpoint;

    @GetMapping("/api/smaato/accept")
    @ResponseBody
    public String accept(@RequestParam("id") int id, @RequestParam(value = "endpoint", required = false) String endpoint) {
        uniqueRequests.computeIfAbsent(id, k -> new LongAdder()).increment();
        return "ok";
    }

    @PostConstruct
    public void init() {
        if (!endpoint.isEmpty()) {
            LOGGER.info("Using endpoint: {}", endpoint);
        }
    }

    @Scheduled(fixedRate = 60000)
    public void logUniqueRequests() {
        int count = uniqueRequests.size();
        uniqueRequests.clear();
        LOGGER.info("Unique requests received in the last minute: {}", count);
        if (!endpoint.isEmpty()) {
            sendCountToEndpoint(count);
        }
    }

    private void sendCountToEndpoint(int count) {
        try {
            Request request = new Request.Builder()
                    .url(endpoint + "?count=" + count)
                    .build();
            Response response = httpClient.newCall(request).execute();
            LOGGER.info("HTTP status code: {}", response.code());
            response.close();
        } catch (Exception e) {
            LOGGER.error("Failed to send count to endpoint", e);
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(RestServiceApplication.class, args);
    }

}
