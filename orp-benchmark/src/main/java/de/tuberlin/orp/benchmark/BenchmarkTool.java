/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Ilya Verbitskiy, Patrick Probst
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.tuberlin.orp.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.RateLimiter;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Request;
import com.ning.http.client.Response;
import io.verbit.ski.core.json.Json;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BenchmarkTool {

  private static AsyncHttpClient httpClient;
  private static AtomicInteger requestsCounter = new AtomicInteger(0);
  private static BenchmarkConfig config;


  public static void main(String[] args) throws Exception {

    // G:/json/CLEF-2015-Task2-Json07/Json-07/2014-07-01.data/2014-07-01.data
    // /Users/ilya/Desktop/2014-07-01.data

    config = new BenchmarkConfig();
    JCommander jCommander = new JCommander(config);
    try {
      jCommander.parse(args);
    } catch (ParameterException e) {
      jCommander.usage();
      System.exit(1);
    }
    int rate = config.getRate();
    int limit = config.getRequestsAmount();
    String filePath = config.getFilePath();


    RateLimiter rateLimiter = RateLimiter.create(rate, 5, TimeUnit.SECONDS);

    httpClient = new AsyncHttpClient();
//    ExecutorService executorService = Executors.newFixedThreadPool(10);

//    Dispatcher dispatcher = new Dispatcher();
//    dispatcher.setMaxRequestsPerHost(concurrentConnections);
//    httpClient.setDispatcher(dispatcher);


    Executors.newSingleThreadScheduledExecutor()
        .scheduleAtFixedRate(() -> {
          System.out.println("Current throughput = " + requestsCounter.get() + " req/s");
          BenchmarkTool.requestsCounter.set(0);
        }, 0, 1000, TimeUnit.MILLISECONDS);

    File file = new File(filePath);
    Stream<String> stringStream = Files.lines(file.toPath(), Charset.defaultCharset());
    List<JsonNode> jsonNodes = stringStream.limit(1000).map(Json::parse).collect(Collectors.toList());

    List<Request> requests = prepareRequests(jsonNodes);

    for (int i = 0; i < limit / 1000; i++) {
      for (Request request : requests) {
        rateLimiter.acquire();
        requestsCounter.incrementAndGet();
        ListenableFuture<Response> requestFuture = httpClient.executeRequest(request);
//        if (request.getFormParams().get(0).getValue().equals("recommendation_request")) {
//          requestFuture.addListener(() -> {
//            try {
//              Response response = requestFuture.get();
//              System.out.println(response.getResponseBody());
//            } catch (InterruptedException | ExecutionException | IOException ignored) {}
//          }, executorService);
//        }
      }
    }

    System.out.println("Done sending.");

  }

  private static List<Request> prepareRequests(List<JsonNode> jsonNodes) {
    List<Request> requests = new ArrayList<>();

    for (JsonNode json : jsonNodes) {
      String url = null;

      String eventType = json.get("event_type").asText();
      switch (eventType) {
        case "impression":
          eventType = "event_notification";
          url = config.getEventUrl();
          break;
        case "recommendation_request":
          url = config.getRequestUrl();
          break;
        default:
          assert false;

      }

      Request request = httpClient.preparePost(url)
          .addFormParam("type", eventType)
          .addFormParam("body", json.toString())
          .build();

      requests.add(request);
    }

    return requests;
  }

}
