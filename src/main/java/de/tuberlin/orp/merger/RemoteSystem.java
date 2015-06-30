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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class RemoteSystem {
  public static void main(String[] args) {

    Config config = ConfigFactory.empty();
    if (args.length == 1) {
      config = ConfigFactory.parseString(args[0]);
    }
    config = config.withFallback(ConfigFactory.load().getConfig("master"));

    ActorSystem remoteSystem = ActorSystem.create("RemoteSystem", config);
    //TODO
//    remoteSystem.actorOf(MostPopularActor.create(0, 0), "mp");
//    remoteSystem.actorOf(CentralOrpActor.create(), "orp");
    ActorRef filterActor = remoteSystem.actorOf(RecommendationFilter.create(), "filter");
    ActorRef mergerActor = remoteSystem.actorOf(MostPopularMerger.create(filterActor), "merger");
  }
}
