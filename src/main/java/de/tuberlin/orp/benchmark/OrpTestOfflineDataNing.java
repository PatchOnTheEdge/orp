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

public class OrpTestOfflineDataNing {
  private static String formatRecReq =
      "{\"recs\": {\"ints\": {\"3\": [186337307, 183665084, 186097159, 186236234, 180818566, 186374324]}}, " +
          "\"event_type\": \"recommendation_request\", \"context\": {\"simple\": {\"62\": 1979832, \"63\": 1840689, " +
          "\"49\": 48, \"67\": 1928642, \"68\": 1851453, \"69\": 1851422, \"24\": 2, \"25\": 186312774, \"27\": %d," +
          " \"22\": 61970, \"23\": 23, \"47\": 654013, \"44\": 1851485, \"42\": 0, \"29\": 17332, \"40\": 1788350, " +
          "\"41\": 25, \"5\": 280, \"4\": 40293, \"7\": 18873, \"6\": 952253, \"9\": 26889, \"13\": 2, \"76\": 1, " +
          "\"75\": 1919968, \"74\": 1919860, \"39\": 748, \"59\": 1275566, \"14\": 33331, \"17\": 48985, \"16\": " +
          "48811, \"19\": 52193, \"18\": 5, \"57\": 71131568, \"56\": 1138207, \"37\": 1978123, \"35\": 315003, " +
          "\"52\": 1, \"31\": 0}, \"clusters\": {\"46\": {\"472375\": 100, \"472420\": 100, \"472376\": 100}, \"51\":" +
          " {\"2\": 255}, \"1\": {\"7\": 255}, \"33\": {\"32962448\": 6, \"2559246\": 2, \"328109\": 8, \"10758\": 1," +
          " \"2033354\": 0, \"556907\": 7, \"236223\": 3, \"4146\": 5, \"39698\": 3, \"399975\": 10, \"60664\": 2, " +
          "\"32941223\": 5, \"7354\": 7, \"101707\": 1, \"2566836\": 1, \"51732\": 2, \"404677\": 5}, \"3\": [55, 28," +
          " 34, 91, 23, 21], \"2\": [11, 11, 61, 60, 61, 26, 21], \"64\": {\"4\": 255}, \"65\": {\"1\": 255}, \"66\":" +
          " {\"12\": 255}}, \"lists\": {\"11\": [13839], \"8\": [18841, 18842, 48511], \"10\": [6, 13, 1768, 1769, " +
          "1770]}}, \"timestamp\": 1404165599401}";
  private static String formatImpression =
      "{\"recs\": {\"ints\": {\"3\": [186460924, 157763723, 186432390, 186381246, 186424478, 165008654]}}, " +
          "\"event_type\": \"impression\", \"context\": {\"simple\": {\"62\": 1918108, \"63\": 1840689, \"49\": 48, " +
          "\"67\": 1928642, \"68\": 1851453, \"69\": 1851422, \"24\": 1, \"25\": 186433311, \"27\": 596, \"22\": " +
          "64929, \"23\": 23, \"47\": 504182, \"44\": 1178658, \"42\": 0, \"29\": 17332, \"40\": 1788375, \"5\": 84, " +
          "\"4\": 1954283, \"7\": 18851, \"6\": 431260, \"9\": 26889, \"13\": 2, \"76\": 1, \"75\": 1954278, \"74\": " +
          "1919947, \"39\": 970, \"59\": 1275566, \"14\": 33331, \"17\": 48985, \"16\": 48811, \"19\": 52193, \"18\":" +
          " 6, \"57\": 4189140733, \"56\": 1138207, \"37\": 1978291, \"35\": 315003, \"52\": 1, \"31\": 0}, " +
          "\"clusters\": {\"46\": {\"472375\": 100, \"472420\": 100, \"472376\": 100}, \"51\": {\"2\": 255}, \"1\": " +
          "{\"7\": 255}, \"33\": {\"2166650\": 1, \"4146\": 2, \"328109\": 8, \"2879388\": 1, \"556907\": 10, " +
          "\"32946259\": 2, \"29313715\": 2, \"21907\": 1, \"399975\": 2, \"49180\": 1, \"17135\": 1, \"1866123\": 4," +
          " \"404677\": 2}, \"3\": [50, 28, 34, 98, 28, 15], \"2\": [21, 11, 50, 74, 60, 23, 11], \"64\": {\"2\": " +
          "255}, \"65\": {\"1\": 255}, \"66\": {\"11\": 255}}, \"lists\": {\"11\": [1806758], \"8\": [18841, 18842], " +
          "\"10\": [4, 6, 1768, 1769, 1770]}}, \"timestamp\": 1404165599468}";
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
            OrpTestOfflineDataNing.requestsCounter.set(0);
          }
        }, 0, rate, TimeUnit.MILLISECONDS);

    File file = new File(filePath);
    Stream<String> stringStream = Files.lines(file.toPath(), Charset.defaultCharset());

    List<JsonNode> collect = stringStream.limit(1000).map(Json::parse).collect(Collectors.toList());

    for (int i = 0; i < limit / 1000; i++) {
      collect.forEach(OrpTestOfflineDataNing::postJson);
    }

    System.out.println("Done sending.");

  }

  private static void postJson(JsonNode json) {
//    System.out.println("json: " + json.toString());
    String eventType = json.get("event_type").asText();
    switch (eventType) {
      case "impression":
        eventType = "event_notification";
    }
//    System.out.println("sending " + eventType);
    String host = "localhost:9000";
//    String host = "irs1.verbit.io";


    if (warmupStep < warmupSteps) {
      double delay = (1 - (warmupStep++ / (double) warmupSteps)) * warmupDelay;
      try {
        Thread.sleep((long) delay);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }


    requestsCounter.incrementAndGet();

    Request request = httpClient.preparePost("http://" + host + "/orp")
        .addFormParam("type", eventType)
        .addFormParam("body", json.toString())
        .build();

    httpClient.executeRequest(request);
  }
}
