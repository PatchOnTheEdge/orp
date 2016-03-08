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
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.tuberlin.orp.common.message.OrpArticle;
import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.message.OrpArticleRemove;
import de.tuberlin.orp.common.message.OrpNotification;
import de.tuberlin.orp.common.message.OrpRequest;
import io.verbit.ski.akka.Akka;
import io.verbit.ski.core.DefaultSkiListener;
import io.verbit.ski.core.Ski;
import io.verbit.ski.core.http.Request;
import io.verbit.ski.core.http.RequestBody;
import io.verbit.ski.core.http.result.AsyncResult;
import io.verbit.ski.core.http.context.RequestContext;
import io.verbit.ski.core.http.result.Result;
import io.verbit.ski.core.json.Json;
import scala.concurrent.Future;

import java.io.File;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static io.verbit.ski.core.http.result.SimpleResult.noContent;
import static io.verbit.ski.core.http.result.SimpleResult.ok;
import static io.verbit.ski.core.plugin.hooks.Hooks.onRequest;
import static io.verbit.ski.core.route.RouteBuilder.post;

public class WorkerServer {
  private static LoggingAdapter log;
  private static boolean LOG_REQUESTS = true;

  public static void main(String[] args) throws Exception {

    String host = "0.0.0.0";
    int port = 9000;


    ActorSystem system = ActorSystem.create("ClusterSystem");
    Cluster cluster = Cluster.get(system);

    log = system.log();
    String master = system.settings().config().getString("master");

    // statistics
    ActorSelection statManagerSel = system.actorSelection(master + "/user/statistics");
    ActorRef statisticsActor = system.actorOf(StatisticsAggregator.create(statManagerSel), "statistics");

    //All Items
    ActorSelection articleMerger = system.actorSelection(master + "/user/articles");
    ActorRef articleAggregator = system.actorOf(ArticleAggregator.create(articleMerger), "articles");

    //Create one worker Actor
    ActorRef workerActor = system.actorOf(WorkerActor.create(statisticsActor, articleAggregator), "orp");


    File itemLogFile = new File("log.txt");
    PrintWriter printWriter = new PrintWriter(itemLogFile);
    log.info("Writing Request Log at " + System.getProperty("user.dir"));

    Ski.builder()
        .setHost(host)
        .setPort(port)
        .addHooks(onRequest((context, next) -> {
              if (LOG_REQUESTS) {
                String request = requestToString(context.request());
                printWriter.print(request + "\n");
                printWriter.flush();
              }
              return next.handle(context);
            }).withDefaultPriority()
        )
        .addRoutes(
            post("/error").route(context -> {
              return forwardError(workerActor, context);
            }),
            post("/event").route(context -> {
              return forwardEvent(workerActor, context);
            }),
            post("/item").route(context -> {
              return forwardItem(workerActor, context);
            }),
            post("/recommendation").routeAsync(context -> {
              return forwardRecommendationRequest(system, statisticsActor, workerActor, context);
            })
    )
        .build()
        .run();
  }



  private static Result forwardError(ActorRef workerActor, RequestContext context) {
    Optional<JsonNode> jsonBody = context.request().formParam("body").asJson();
    JsonNode json = jsonBody.get();

    System.err.println(Instant.now() + ": Received Error: " + json);


    return noContent();
  }

  private static Result forwardItem(ActorRef workerActor, RequestContext context) {
    Optional<JsonNode> jsonBody = context.request().formParam("body").asJson();

    JsonNode json = jsonBody.get();
    int flag = json.get("flag").asInt();

    if ((flag & 1) == 1){
      //Article is recommendable
      OrpArticle article = new OrpArticle(json);
      workerActor.tell(article, ActorRef.noSender());
    }
    else {
      //Article shall be removed
      OrpArticleRemove toRemove = new OrpArticleRemove(json.get("id").asText(), json.get("domainid").asText());
      workerActor.tell(toRemove, ActorRef.noSender());
    }

    return noContent();
  }

  private static AsyncResult forwardRecommendationRequest(ActorSystem system, final ActorRef statisticsActor, ActorRef workerActor, RequestContext context) {
    Optional<String> messageType = context.request().formParam("type").asText();
    Optional<JsonNode> jsonBody = context.request().formParam("body").asJson();

    OrpRequest orpRequest = new OrpRequest(jsonBody.get());

    long start = System.currentTimeMillis();

    Future<Result> future = Patterns.ask(workerActor, orpRequest, 1500)
        .map(new Mapper<Object, Result>() {
          @Override
          public Result apply(Object o) {
//            if (o == null) {
//              return ok(Json.newObject());
//            }

            MostPopularRanking mostPopularRanking = (MostPopularRanking) o;

            if (mostPopularRanking.getRanking().isEmpty()) {
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


            double max = mostPopularRanking.getRanking().values().stream().mapToLong(l -> l).max().getAsLong();

            for (Map.Entry<String, Long> entry : mostPopularRanking.getRanking().entrySet()) {
              items.add(entry.getKey());
              scores.add(entry.getValue() / max);
            }

            long responseTime = System.currentTimeMillis() - start;
            statisticsActor.tell(new StatisticsAggregator.ResponseTime(responseTime), ActorRef.noSender());

            log.info("Sending Recommendation = " + result);
            return ok(result);
          }
        }, system.dispatcher());


    return Akka.wrap(future, system.dispatcher());
  }

  private static Result forwardEvent(ActorRef workerActor, RequestContext context) {
    Optional<String> messageType = context.request().formParam("type").asText();
    Optional<JsonNode> jsonBody = context.request().formParam("body").asJson();

    OrpContext orpContext = new OrpContext(jsonBody.get());
    OrpNotification notification = new OrpNotification(messageType.get(), orpContext);

    workerActor.tell(notification, ActorRef.noSender());

    return noContent();
  }

  private static String requestToString(Request request){
    Map<String, List<String>> bodyForm = request.body().asForm();
    JsonNode json = Json.newObject();
    String bodyString = "";
    if (!bodyForm.isEmpty()) {
      bodyString = queryParamsToString(bodyForm);
      json = Json.parse(bodyString);
    }
    String path = request.path();
    String queryString = queryParamsToString(request.queryParams());
    Date date = new Date();
    ObjectNode result = Json.newObject().put("path", path)
        .put("queryString", queryString).put("timestamp", date.toString());
    result.set("body", json);
    return result.toString();
  }

  private static String queryParamsToString(Map<String, List<String>> map){
    return map.entrySet().stream()
        .flatMap(stringListEntry -> stringListEntry.getValue().stream())
        .collect(Collectors.joining(","));
  }
}
