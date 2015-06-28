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

package de.tuberlin.orp.akka.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.Broadcast;
import de.tuberlin.orp.core.OrpContext;
import de.tuberlin.orp.core.Ranking;
import de.tuberlin.orp.core.OrpContextCounter;
import scala.concurrent.duration.Duration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An actor that can receive items and store them per publisher in a private Hashmap. The top n entries will be send to
 * the MostPopularMerger in a defined interval.
 */
public class MostPopularActor extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private OrpContextCounter contextCounter;

  private long lastRanking = 0;
  private int receivedImpressions = 0;


  /**
   * @see #MostPopularActor(int, int)
   */
  public static Props create(int contextWindowSize, int topListSize) {
    return Props.create(MostPopularActor.class, () -> {
      return new MostPopularActor(contextWindowSize, topListSize);
    });
  }

  /**
   * @param contextWindowSize
   *     The window size which defines of how many contexts a window should consist.
   * @param topListSize
   *     The maximum size of the rankings for the publishers.
   */
  public MostPopularActor(int contextWindowSize, int topListSize) {
    this.contextCounter = new OrpContextCounter(contextWindowSize, topListSize);
  }


  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpContext) {
      OrpContext context = (OrpContext) message;

//      log.info(String.format("Received Item from Publisher: %s with ID: %s",
//          context.getContext(), context.getItemId()));

      ++receivedImpressions;
      contextCounter.add(context);

    } else if (message.equals("getRankings")) {
      log.debug("received Broadcast message");
      if (lastRanking != 0) {
        double throughput = 1000.0 * receivedImpressions / (System.currentTimeMillis() - lastRanking);
        log.info(String.format("Throughput: %.2f impressions/sec", throughput));
      }

      lastRanking = System.currentTimeMillis();
      receivedImpressions = 0;

      Map<String, Ranking> rankings = contextCounter.getRankings();
//          log.info(rankings.toString());

      getSender().tell(new MostPopularMerger.Merge(rankings), getSelf());
    }
  }
}
