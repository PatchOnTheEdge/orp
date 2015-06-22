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

package de.tuberlin.orp;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Mapper;
import akka.pattern.Patterns;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.tuberlin.orp.akka.actors.CentralOrpActor;
import de.tuberlin.orp.core.OrpContext;
import de.tuberlin.orp.core.Ranking;
import io.verbit.ski.akka.Akka;
import io.verbit.ski.core.Ski;
import io.verbit.ski.core.http.Result;
import io.verbit.ski.core.json.Json;
import scala.concurrent.Future;

import java.util.Map;
import java.util.Optional;

import static io.verbit.ski.core.http.AsyncResult.async;
import static io.verbit.ski.core.http.SimpleResult.badRequest;
import static io.verbit.ski.core.http.SimpleResult.noContent;
import static io.verbit.ski.core.http.SimpleResult.ok;
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
            get("/test").route(context -> ok("Everything running.")),
            post("/orp").routeAsync(context -> {
              Optional<String> messageType = context.request().formParam("type").asText();
              Optional<JsonNode> jsonBody = context.request().formParam("body").asJson();

//              System.out.println("message: " + messageType.get());

//              System.out.println("type = " + messageType.get());
//              System.out.println("jsonBody = " + jsonBody.get().toString());


              if (messageType.isPresent() && jsonBody.isPresent()) {

                OrpContext orpContext = new OrpContext(jsonBody.get());
                CentralOrpActor.OrpNotification notification =
                    new CentralOrpActor.OrpNotification(messageType.get(), orpContext);

                switch (messageType.get()) {
                  //The three subtypes of an event notification:
                  case "item_update":
                  case "event_notification":
                    centralOrpActor.tell(notification, ActorRef.noSender());
                    return async(noContent());
                  case "recommendation_request":
                    CentralOrpActor.OrpRequest request = new CentralOrpActor.OrpRequest(orpContext);

                    Future<Result> ask = Patterns.ask(centralOrpActor, request, 80)
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
                            return ok(result);
                          }
                        }, system.dispatcher());

                    return Akka.wrap(ask);
                  default:
                    return async(badRequest("Unknown message type: " + messageType.get()));
                }
              }

              return async(badRequest("No message type found."));
            })
        )
        .build()
        .start();

  }
}
