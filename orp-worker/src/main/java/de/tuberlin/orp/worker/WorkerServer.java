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
import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.message.OrpNotification;
import io.verbit.ski.core.Ski;

import java.util.Optional;

import static io.verbit.ski.core.http.AsyncResult.async;
import static io.verbit.ski.core.http.SimpleResult.badRequest;
import static io.verbit.ski.core.http.SimpleResult.noContent;
import static io.verbit.ski.core.http.SimpleResult.ok;
import static io.verbit.ski.core.route.RouteBuilder.post;

public class WorkerServer {

  public static void main(String[] args) throws Exception {
    String host = "0.0.0.0";
    int port = 9000;

    ActorSystem system = ActorSystem.create("OrpSystem");
    ActorRef workerActor = system.actorOf(WorkerActor.create(), "orp");

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
            })
        )
        .build()
        .start();

  }
}
