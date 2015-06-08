/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Ilya Verbitskiy
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

package io.verbit.orp;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.verbit.orp.akka.actors.CentralOrpActor;
import io.verbit.orp.core.Context;
import io.verbit.orp.core.Ranking;
import io.verbit.ski.akka.AkkaUtils;
import io.verbit.ski.core.Ski;
import io.verbit.ski.core.json.Json;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.verbit.ski.core.http.HttpContext.request;
import static io.verbit.ski.core.http.Result.badRequest;
import static io.verbit.ski.core.http.Result.noContent;
import static io.verbit.ski.core.http.Result.ok;
import static io.verbit.ski.core.route.RouteBuilder.get;
import static io.verbit.ski.core.route.RouteBuilder.post;

public class Orp {
  private static final ActorSystem system = ActorSystem.create("MySystem");
  private static ActorRef centralOrpActor = system.actorOf(CentralOrpActor.props(2), "orp");

  public static void main(String[] args) throws Exception {
    String host = "0.0.0.0";
    int port = 9000;
    for (String arg : args) {
      System.out.println(arg);
    }
    if (args.length == 1) {
      String[] split = args[0].split(":");
      host = split[0];
      port = Integer.parseInt(split[1]);
    }

    Ski.builder()
        .setHost(host)
        .setPort(port)
        .addRoutes(
            get("/test").route(() -> {
              return ok("Everything running.");
            }).build(),
            post("/orp").route(() -> {
              Optional<String> messageType = request().formParam("type").asText();
              Optional<JsonNode> jsonBody = request().formParam("body").asJson();

//              System.out.println("message: " + messageType.get());

//              System.out.println("type = " + messageType.get());
//              System.out.println("jsonBody = " + jsonBody.get().toString());


              if (messageType.isPresent() && jsonBody.isPresent()) {

                Context context = new Context(jsonBody.get());
                CentralOrpActor.OrpNotification notification =
                    new CentralOrpActor.OrpNotification(messageType.get(), context);

                switch (messageType.get()) {
                  //The three subtypes of an event notification:
                  case "item_update":
                  case "event_notification":
                    centralOrpActor.tell(notification, ActorRef.noSender());
                    return CompletableFuture.completedFuture(noContent());
                  case "recommendation_request":
                    CentralOrpActor.OrpRequest request = new CentralOrpActor.OrpRequest(context);
                    return AkkaUtils.wrapFuture(Patterns.ask(centralOrpActor, request, 80))
                        .thenApply(o -> {
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
                          return ok(result);
                        });
                  default:
                    return CompletableFuture.completedFuture(badRequest("Unknown message type: " + messageType.get()));
                }
              }

              return CompletableFuture.completedFuture(badRequest("No message type found."));
            }).build()
        )
        .build()
        .start();

  }
}
