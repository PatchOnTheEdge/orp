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

package de.tuberlin.orp.worker;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import akka.dispatch.Mapper;
import akka.pattern.Patterns;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.tuberlin.orp.common.Ranking;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.message.OrpItemUpdate;
import de.tuberlin.orp.common.message.OrpNotification;
import de.tuberlin.orp.common.message.OrpRequest;
import io.verbit.ski.akka.Akka;
import io.verbit.ski.core.Ski;
import io.verbit.ski.core.http.Result;
import io.verbit.ski.core.json.Json;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.Map;
import java.util.Optional;

import static io.verbit.ski.core.http.SimpleResult.noContent;
import static io.verbit.ski.core.http.SimpleResult.ok;
import static io.verbit.ski.core.route.RouteBuilder.post;

public class WorkerServer {

  public static void main(String[] args) throws Exception {
    String host = "0.0.0.0";
    int port = 9000;


    ActorSystem system = ActorSystem.create("ClusterSystem");
    Cluster cluster = Cluster.get(system);

    String master = system.settings().config().getString("master");

    // statistics
    ActorSelection statManagerSel = system.actorSelection(master + "/user/statistics");
    Future<ActorRef> actorRefFuture = statManagerSel.resolveOne((FiniteDuration) Duration.create("100ms"));
    ActorRef statisticsActor = system.actorOf(StatisticsAggregator.create(statManagerSel), "statistics");


    ActorRef workerActor = system.actorOf(WorkerActor.create(statisticsActor), "orp");


    Ski.builder()
        .setHost(host)
        .setPort(port)
        .addRoutes(
            post("/event").route(context -> {
              Optional<String> messageType = context.request().formParam("type").asText();
              Optional<JsonNode> jsonBody = context.request().formParam("body").asJson();

              OrpContext orpContext = new OrpContext(jsonBody.get());
              OrpNotification notification = new OrpNotification(messageType.get(), orpContext);

              workerActor.tell(notification, ActorRef.noSender());

              return noContent();
            }),
            post("/recommendation").routeAsync(context -> {
              Optional<String> messageType = context.request().formParam("type").asText();
              Optional<JsonNode> jsonBody = context.request().formParam("body").asJson();

              OrpContext orpContext = new OrpContext(jsonBody.get());
              OrpRequest orpRequest = new OrpRequest(jsonBody.get());

              long start = System.currentTimeMillis();

              Future<Result> future = Patterns.ask(workerActor, orpRequest, 1000)
                  .map(new Mapper<Object, Result>() {
                    @Override
                    public Result apply(Object o) {
                      if (o == null) {
                        return ok(Json.newObject());
                      }

                      Ranking ranking = (Ranking) o;

                      if (ranking.getRanking().isEmpty()) {
                        return ok(Json.newObject());
                      }

                      ObjectNode result = Json.newObject();
                      ObjectNode recs = result.putObject("recs");

                      ArrayNode items = recs
                          .putObject("ints")
                          .putArray("3");

                      ArrayNode scores = recs
                          .putObject("floats")
                          .putArray("2");


                      double max = ranking.getRanking().values().stream().mapToLong(l -> l).max().getAsLong();

                      for (Map.Entry<String, Long> entry : ranking.getRanking().entrySet()) {
                        items.add(entry.getKey());
                        scores.add(entry.getValue() / max);
                      }

                      long responseTime = System.currentTimeMillis() - start;
                      statisticsActor.tell(new StatisticsAggregator.ResponseTime(responseTime), ActorRef.noSender());

                      return ok(result);
                    }
                  }, system.dispatcher());


              return Akka.wrap(future);
            }),
            post("/item").route(context -> {
              Optional<String> messageType = context.request().formParam("type").asText();
              Optional<JsonNode> jsonBody = context.request().formParam("body").asJson();

              JsonNode json = jsonBody.get();
              String itemId = json.get("id").asText();
              int flag = json.get("flag").asInt();

              OrpItemUpdate itemUpdate = new OrpItemUpdate(itemId, flag);

              workerActor.tell(itemUpdate, ActorRef.noSender());

              return noContent();
            })
        )
        .build()
        .start();

  }
}
