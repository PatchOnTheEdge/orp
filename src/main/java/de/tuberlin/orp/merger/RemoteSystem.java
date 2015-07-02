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

package de.tuberlin.orp.merger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Mapper;
import akka.pattern.Patterns;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.verbit.ski.akka.Akka;
import io.verbit.ski.core.Ski;
import io.verbit.ski.core.http.Result;
import io.verbit.ski.core.json.Json;
import scala.concurrent.Future;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.verbit.ski.core.http.SimpleResult.ok;
import static io.verbit.ski.core.route.RouteBuilder.get;
import static io.verbit.ski.template.jtwig.JtwigTemplateResult.render;

public class RemoteSystem {

  public static void main(String[] args) throws Exception {
    String host = "0.0.0.0";
    int port = 9001;

    Config config = ConfigFactory.empty();
    if (args.length == 1) {
      config = ConfigFactory.parseString(args[0]);
    }
    config = config.withFallback(ConfigFactory.load().getConfig("master"));

    ActorSystem remoteSystem = ActorSystem.create("RemoteSystem", config);
    ActorRef filterActor = remoteSystem.actorOf(RecommendationFilter.create(), "filter");
    ActorRef mergerActor = remoteSystem.actorOf(MostPopularMerger.create(filterActor), "merger");
    ActorRef statisticsActor = remoteSystem.actorOf(CentralStatisticsActor.create(), "statistics");

    Ski.builder()
        .setHost(host)
        .setPort(port)
        .addRoutes(
            get("/report").routeAsync(context -> {
              Future<Result> result =
                  Patterns.ask(statisticsActor, new CentralStatisticsActor.RetrieveStatisticsReport(), 100)
                  .map(new Mapper<Object, Result>() {
                    @Override
                    public Result apply(Object parameter) {
                      StringBuilder csv = new StringBuilder();
                      CentralStatisticsActor.StatisticsReport report = (CentralStatisticsActor.StatisticsReport)
                          parameter;
                      LinkedHashMap<ActorRef, ArrayDeque<CentralStatisticsActor.WorkerStatistics>> workerStats =
                          report.getWorkerStatistics();

                      csv.append("Throughput\n");
                      for (ActorRef actorRef : workerStats.keySet()) {
                        csv.append(actorRef.toString());
                        csv.append('\n');
                        ArrayDeque<CentralStatisticsActor.WorkerStatistics> wStats = workerStats.get(actorRef);

                        Iterator<CentralStatisticsActor.WorkerStatistics> it = wStats.descendingIterator();
                        while (it.hasNext()) {
                          CentralStatisticsActor.WorkerStatistics stat = it.next();
                          csv.append(stat.getTimestamp());
                          csv.append(';');
                          csv.append((long) stat.getThroughput());
                          csv.append('\n');
                        }

                        csv.append('\n');
                        csv.append('\n');
                      }


                      csv.append("Response Time\n");
                      ArrayDeque<CentralStatisticsActor.MergerStatistics> resStats = report.getResponseTimes();
                      Iterator<CentralStatisticsActor.MergerStatistics> it = resStats.descendingIterator();
                      while (it.hasNext()) {
                        CentralStatisticsActor.MergerStatistics stat = it.next();
                        csv.append(stat.getTimestamp());
                        csv.append(';');
                        csv.append(stat.getResponseTime());
                        csv.append('\n');
                      }

                      return ok(csv.toString());
                    }
                  }, remoteSystem.dispatcher());
              return Akka.wrap(result);
            }),
            get("/statistics").routeAsync(context -> {
              Future<Result> result =
                  Patterns.ask(statisticsActor, new CentralStatisticsActor.RetrieveStatistics(), 100)
                      .map(new Mapper<Object, Result>() {
                        @Override
                        public Result apply(Object parameter) {
//                      StringBuilder throughputRow = new StringBuilder();
//                      List<CentralStatisticsActor.WorkerStatistics> statisticsList =
//                          ((CentralStatisticsActor.StatisticsMessage) parameter).getWorkerStatistics();
                          CentralStatisticsActor.StatisticsMessage stats = (CentralStatisticsActor
                              .StatisticsMessage) parameter;
                          LinkedHashMap<ActorRef, CentralStatisticsActor.WorkerStatistics> workerStats =
                              stats.getWorkerStatistics();

//                      for (CentralStatisticsActor.WorkerStatistics statistics : statisticsList) {
//                        throughputRow.append(statistics.getThroughput());
//                        throughputRow.append('\n');
//                      }

                          ObjectNode result = Json.newObject();
                          ArrayNode workers = result.putArray("workers");
                          for (Map.Entry<ActorRef, CentralStatisticsActor.WorkerStatistics> statsEntries :
                              workerStats.entrySet()) {
                            workers.add(
                                Json.newObject()
                                    .put("timestamp", statsEntries.getValue().getTimestamp())
                                    .put("cpu", statsEntries.getValue().getCpu())
                                    .put("throughput", statsEntries.getValue().getThroughput())
                            );
                          }


                          CentralStatisticsActor.MergerStatistics responseTimeStatistics = stats
                              .getResponseTimeStatistics();
                          if (responseTimeStatistics != null) {
                            result.putObject("master")
                                .put("timestamp", responseTimeStatistics.getTimestamp())
                                .put("responseTime", responseTimeStatistics.getResponseTime());
                          }


                          return ok(result);
                        }
                      }, remoteSystem.dispatcher());
              return Akka.wrap(result);
            }),
            get("/").route(context -> render("templates/index.twig", Json.newObject()))
        )
        .build()
        .start();

  }
}
