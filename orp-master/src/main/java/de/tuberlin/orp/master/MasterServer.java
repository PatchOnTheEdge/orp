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
import io.verbit.ski.akka.Akka;
import io.verbit.ski.core.Ski;
import io.verbit.ski.core.http.Result;
import io.verbit.ski.core.json.Json;
import scala.concurrent.Future;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;

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

    Ski.builder()
        .setHost(host)
        .setPort(port)
        .addRoutes(
            get("/report").routeAsync(context -> {
              Future<Result> result =
                  Patterns.ask(statisticsManager, "getStatisticsReport", 100)
                      .map(new Mapper<Object, Result>() {
                        @Override
                        public Result apply(Object parameter) {
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

                          return ok(csv.toString());
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
            get("/").route(context -> render("templates/index.twig", Json.newObject()))
        )
        .build()
        .start();

  }
}
