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

package de.tuberlin.orp.master;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import akka.dispatch.Mapper;
import akka.pattern.Patterns;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.tuberlin.orp.common.rankings.MostPopularRanking;
import de.tuberlin.orp.common.messages.OrpItemUpdate;
import io.verbit.ski.akka.Akka;
import io.verbit.ski.core.Ski;
import io.verbit.ski.core.http.Result;
import io.verbit.ski.core.json.Json;
import scala.concurrent.Future;

import java.util.*;

import static io.verbit.ski.core.http.SimpleResult.ok;
import static io.verbit.ski.core.route.RouteBuilder.get;
import static io.verbit.ski.template.jtwig.JtwigTemplateResult.render;
public class MasterServer {

  public static void main(String[] args) throws Exception {
    String host = "0.0.0.0";
    int port = 9001;
    ActorSystem system = ActorSystem.create("ClusterSystem");
    Cluster cluster = Cluster.get(system);
    ActorRef mergerActor = system.actorOf(MostPopularMerger.create(), "merger");
    ActorRef statisticsManager = system.actorOf(StatisticsManager.create(), "statistics");
    ActorRef itemHandler = system.actorOf(ItemHandler.create(),"items");
    Ski.builder()
        .setHost(host)
        .setPort(port)
        .setStaticFolder(MasterServer.class.getClassLoader().getResource("web").getPath())
        .addRoutes(

            get("/").route(context -> render("web/index.html", Json.newObject())),

            get("/report").routeAsync(context -> {
              Future<Result> result =
                  Patterns.ask(statisticsManager, "getStatisticsReport", 100)
                      .map(new Mapper<Object, Result>() {

                        @Override
                        public Result apply(Object parameter) {
                          return ok(buildReport(parameter));
                        }
                      }, system.dispatcher());
              return Akka.wrap(result);
            }),

            get("/statistics").routeAsync(context -> {
              Future<Result> result =
                  Patterns.ask(statisticsManager, "getCurrentStatistics", 100)
                      .map(new Mapper<Object, Result>() {
                        @Override
                        public Result apply(Object parameter) {
                          StatisticsManager.StatisticsMessage stats = (StatisticsManager
                              .StatisticsMessage) parameter;
                          LinkedHashMap<ActorRef, StatisticsManager.WorkerStatistics> workerStats =
                              stats.getWorkerStatistics();

                          ObjectNode result = Json.newObject();
                          ArrayNode workers = result.putArray("workers");
                          for (Map.Entry<ActorRef, StatisticsManager.WorkerStatistics> statsEntries :
                              workerStats.entrySet()) {
                            workers.add(
                                Json.newObject()
                                    .put("timestamp", statsEntries.getValue().getTimestamp())
                                    .put("throughput", statsEntries.getValue().getThroughput())
                            );
                          }

                          return ok(result);
                        }
                      }, system.dispatcher());
              return Akka.wrap(result);
            }),

            get("/throughput").route(context -> render("web/templates/index.twig", Json.newObject())),

            get("/items").routeAsync(context -> {
              Future<Result> result =
                  Patterns.ask(itemHandler, "getItems", 100)
                      .map(new Mapper<Object, Result>() {
                        @Override
                        public Result apply(Object parameter) {

                          ObjectNode result = Json.newObject();
                          ArrayNode items = result.putArray("items");

                          Map<String, Map<String, OrpItemUpdate>> publisherItems = (Map<String, Map<String, OrpItemUpdate>>) parameter;

                          for (String publisher : publisherItems.keySet()) {
                            buildItemArray(items, publisherItems.get(publisher));
                          }

                          return ok(result);
                        }
                      }, system.dispatcher());
              return Akka.wrap(result);
            }),

            get("/ranking-mp").routeAsync(context -> {
              Future<Result> result =
                  Patterns.ask(mergerActor, "getMergerResult", 100)
                      .map(new Mapper<Object, Result>() {

                        @Override
                        public Result apply(Object object) {

                          Map<String, MostPopularRanking> pubRankMap = (Map<String, MostPopularRanking>) object;
                          Set<String> publishers = pubRankMap.keySet();

                          ObjectNode result = Json.newObject();
                          ArrayNode data = result.putArray("rankings");

                          for (String publisher : publishers) {
                            MostPopularRanking mostPopularRanking = pubRankMap.get(publisher);

                            ObjectNode publisherNode = Json.newObject();
                            publisherNode.put("publisherId", publisher);
                            ArrayNode rankingNode = publisherNode.putArray("ranking");
                            rankingNode.addAll(mostPopularRanking.toJson());

                            data.add(publisherNode);
                          }
                          return ok(result);
                        }
                      }, system.dispatcher());
              return Akka.wrap(result);
            })
        )
        .build()
        .start();
  }

  /**
   * Writes throughput and response time in csv-like style
   * @param parameter
   * @return String containing throughput and response time
   */
  private static String buildReport(Object parameter) {
    StringBuilder csv = new StringBuilder();

    StatisticsManager.StatisticsReport report = (StatisticsManager.StatisticsReport) parameter;
    LinkedHashMap<ActorRef, ArrayDeque<StatisticsManager.WorkerStatistics>> workerStats =
        report.getWorkerStatistics();

    csv.append("Throughput\n");

    for (ActorRef actorRef : workerStats.keySet()) {

      csv.append(actorRef.toString())
          .append('\n');

      ArrayDeque<StatisticsManager.WorkerStatistics> wStats = workerStats.get(actorRef);

      Iterator<StatisticsManager.WorkerStatistics> it = wStats.descendingIterator();
      while (it.hasNext()) {
        StatisticsManager.WorkerStatistics stat = it.next();
        csv.append(stat.getTimestamp())
            .append(';')
            .append((long) stat.getThroughput())
            .append('\n');
      }
      csv.append("\n\n");
    }

    csv.append("Response Time\n");
    SortedMap<Short, Long> resStats = report.getResponseTimes();
    for (Map.Entry<Short, Long> entry : resStats.entrySet()) {
      csv.append(entry.getKey());
      csv.append(';');
      csv.append(entry.getValue());
      csv.append('\n');
    }
    return csv.toString();
  }

  /**
   * Builds a Json Array which contains all items from one publisher
   * @param data The Array Node which will contain the items
   * @param items The Hashmap which holds the items
   */
  private static void buildItemArray(ArrayNode data, Map<String, OrpItemUpdate> items){
    for (String itemId : items.keySet()) {
      OrpItemUpdate item = items.get(itemId);
      //System.out.println("providing item with id = " + itemId);
      ObjectNode itemJson = Json.newObject();
      ObjectNode node = item.getJson();
      itemJson.put("itemId", itemId);
      itemJson.put("item", node);
      data.add(itemJson);
    }
  }
}
