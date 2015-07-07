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
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import io.verbit.ski.core.json.Json;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileRequestProvider implements RequestProvider {
  private List<Request> requests;
  private Iterator<Request> requestIterator;
  private BenchmarkConfig config;

  public FileRequestProvider(String filePath, BenchmarkConfig config) {
    this.config = config;
    File file = new File(filePath);
    try {
      Stream<String> stringStream = Files.lines(file.toPath(), Charset.defaultCharset());
      List<JsonNode> jsonNodes = stringStream.limit(1000).map(Json::parse).collect(Collectors.toList());
      requests = prepareRequests(jsonNodes);
      requestIterator = requests.iterator();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Request getNext() {
    if (!requestIterator.hasNext()) {
      requestIterator = requests.iterator();
    }

    return requestIterator.next();
  }

  private List<Request> prepareRequests(List<JsonNode> jsonNodes) {
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

      new RequestBuilder();


      Request request = new RequestBuilder("POST")
          .addFormParam("type", eventType)
          .addFormParam("body", json.toString())
          .build();

      requests.add(request);
    }

    return requests;
  }
}
