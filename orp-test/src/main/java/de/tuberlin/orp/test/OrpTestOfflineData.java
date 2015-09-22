package de.tuberlin.orp.test;/*
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


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.TreeNode;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import io.verbit.ski.core.json.Json;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
public class OrpTestOfflineData {

  public static String HOST = "localhost";
      //"37.120.189.25";
  public static void main(String[] args) throws Exception {
    // C:\Users\Patch\projects\orp\test.data.json C:\Users\Patch\projects\orp\test.item.json 5 5
    // /Users/ilya/Desktop/2014-07-01.data
    // C:\Users\Patch\projects\json\CLEF-2015-Task2-Json07\Json-07\2014-07-01.data\2014-07-01.data C:\Users\Patch\projects\json\CLEF-2015-Task2-Json07\Json-07\2014-07-01.items\2014-07-01.items

    String filePathData = args[0];
    String filePathItem = args[1];
    int limitData = Integer.parseInt(args[2]);
    int limitItem = Integer.parseInt(args[3]);

    File fileData = new File(filePathData);
    File fileItem = new File(filePathItem);

    Stream<String> stringStreamData = Files.lines(fileData.toPath(), Charset.defaultCharset());
    Stream<String> stringStreamItem = Files.lines(fileItem.toPath(), Charset.defaultCharset());
    List<JsonNode> jsonNodes = stringStreamData.limit(limitData).map(Json::parse).collect(Collectors.toList());
    List<JsonNode> jsonNodes2 = stringStreamItem.limit(limitItem).map(Json::parse).collect(Collectors.toList());

    jsonNodes.forEach(OrpTestOfflineData::postJson);
    System.out.println("Done sending Data.");
    jsonNodes2.forEach(OrpTestOfflineData::postJsonItem);
    System.out.println("Done sending Items.");

  }

  private static void postJsonItem(JsonNode jsonNode) {
    //System.out.println("json Item: " + jsonNode.toString());
    String id = jsonNode.get("id").asText();
    String title = jsonNode.get("title").asText();
    String flag = jsonNode.get("flag").asText();

    Future<HttpResponse<String>> httpResponseFuture = Unirest.post("http://"+HOST+":9000/item")
        .field("body", jsonNode.toString())
        .asStringAsync();
  }

  private static void postJson(JsonNode json) {
    //System.out.println("json Data: " + json.toString());
    String eventType;
    eventType = json.get("event_type").asText();
    switch (eventType) {
      case "impression":
        eventType = "event_notification";
    }
    //System.out.println("sending " + eventType);
    Future<HttpResponse<String>> httpResponseFuture = Unirest.post("http://"+HOST+":9000/event")
        .field("type", eventType)
        .field("body", json.toString())
        .asStringAsync();

    if (eventType.equalsIgnoreCase("recommendation_request")) {
      Runnable onCompleted = () -> {
        try {
          HttpResponse<String> httpResponse = httpResponseFuture.get();
          printResponse(httpResponse);
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      };
      new Thread(onCompleted).start();
    }
//    System.out.println("Http Respone Status: " + httpResponse.getStatus());
  }

  private synchronized static void printResponse(HttpResponse<?> response) {
    System.out.printf("%d - %s%n", response.getStatus(), response.getStatusText());
    //System.out.println(response.getBody());
    //System.out.println(response.getHeaders().toString());
    //System.out.println();
  }
}