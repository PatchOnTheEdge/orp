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

import com.fasterxml.jackson.databind.JsonNode;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Request;
import io.verbit.ski.core.json.Json;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrpTestOfflineData {
  private static AsyncHttpClient httpClient;

  private static int warmupSteps = 500;
  private static int warmupStep = 0;

  private static int warmupDelay = 10;

  private static AtomicInteger requestsCounter = new AtomicInteger(0);


  public static void main(String[] args) throws Exception {

    // G:/json/CLEF-2015-Task2-Json07/Json-07/2014-07-01.data/2014-07-01.data
    // /Users/ilya/Desktop/2014-07-01.data

    String filePath = args[0];
    int limit = Integer.parseInt(args[1]);


    httpClient = new AsyncHttpClient();
//    Dispatcher dispatcher = new Dispatcher();
//    dispatcher.setMaxRequestsPerHost(concurrentConnections);
//    httpClient.setDispatcher(dispatcher);

    int rate = 2000;

    Executors.newSingleThreadScheduledExecutor()
        .scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            System.out.println("Current throughput = " + (requestsCounter.get() / (double) rate) * 1000 + " req/s");
            OrpTestOfflineData.requestsCounter.set(0);
          }
        }, 0, rate, TimeUnit.MILLISECONDS);

    File file = new File(filePath);
    Stream<String> stringStream = Files.lines(file.toPath(), Charset.defaultCharset());
    List<JsonNode> collect = stringStream.limit(1000).map(Json::parse).collect(Collectors.toList());

    for (int i = 0; i < limit / 1000; i++) {
      collect.forEach(OrpTestOfflineData::postJson);
    }

    System.out.println("Done sending.");

  }

  private static void postJson(JsonNode json) {

    String eventType = json.get("event_type").asText();
    switch (eventType) {
      case "impression":
        eventType = "event_notification";
    }

    Request request = httpClient.preparePost("http://irs1.verbit.io/orp")
        .addFormParam("type", eventType)
        .addFormParam("body", json.toString())
        .build();

//    Future<HttpResponse<String>> httpResponseFuture = Unirest.post("http://" + host + "/orp")
//				.field("type", eventType)
//				.field("body", json.toString())
//				.asStringAsync();

    if (warmupStep < warmupSteps) {
      double delay = (1 - (warmupStep++ / (double) warmupSteps)) * warmupDelay;
      try {
        Thread.sleep((long) delay);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }


    requestsCounter.incrementAndGet();

    httpClient.executeRequest(request);
  }
}
