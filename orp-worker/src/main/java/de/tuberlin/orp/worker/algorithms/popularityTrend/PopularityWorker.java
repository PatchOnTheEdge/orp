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

package de.tuberlin.orp.worker.algorithms.popularityTrend;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.repository.RankingRepository;
import de.tuberlin.orp.worker.RequestCoordinator;
import de.tuberlin.orp.worker.algorithms.mostPopular.OrpContextCounter;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * An actor that can receive items and store them per publisher in a private Hashmap. The top n entries will be send to
 * the MostPopularMerger in a defined interval.
 */
public class PopularityWorker extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private TrendRepository trendRepository;
  private int intervalMinutes;


  static class PopularityActorCreator implements Creator<PopularityWorker> {

    private int counterSize;
    private int topListSize;
    private int windowSize;
    private int intervalMinutes;

    public PopularityActorCreator(int counterSize, int windowSize, int topListSize, int intervalMinutes) {
      this.counterSize = counterSize;
      this.windowSize = windowSize;
      this.topListSize = topListSize;
      this.intervalMinutes = intervalMinutes;
    }

    @Override
    public PopularityWorker create() throws Exception {
      return new PopularityWorker(counterSize, windowSize, topListSize, intervalMinutes);
    }
  }

  /**
   * @see #PopularityWorker(int, int, int, int)
   */
  public static Props create(int counterSize, int windowSize, int topListSize, int intervalMinutes) {
    return Props.create(PopularityWorker.class, new PopularityActorCreator(counterSize, windowSize, topListSize, intervalMinutes));
  }

  /**
   * @param counterSize
   *     The size of a single Counter which defines of how many contexts a counter can contain.
   * @param windowSize
   *     The window size which defines how many counters can be hold.
   * @param topListSize
   *     The maximum size of the ranking for the publishers.
   * @param intervalMinutes
   *     The length of an intervall in minutes
   */
  public PopularityWorker(int counterSize, int windowSize, int topListSize, int intervalMinutes) {
    this.intervalMinutes = intervalMinutes;
    this.trendRepository = new TrendRepository(counterSize, windowSize, topListSize, intervalMinutes);
  }

  @Override
  public void preStart() throws Exception {
    log.info("Popularity Worker started.");
    getContext().system().scheduler().schedule(Duration.create(30, TimeUnit.MINUTES), Duration.create(intervalMinutes, TimeUnit.MINUTES), trendRepository::newInterval, getContext().dispatcher());
    super.preStart();
    log.info(getSelf().toString());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpContext) {
      OrpContext context = (OrpContext) message;

//      log.info(String.format("Received OrpArticle from Publisher: %s with ID: %s",
//          context.getContext(), context.getItemId()));

      trendRepository.add(context);

    } else if (message.equals("getIntermediateRanking")) {

      RankingRepository rankingRepository = trendRepository.getRankingRespository();
      //log.info("Intermediate ranking requested.");
      getSender().tell(new RequestCoordinator.IntermediateRanking(rankingRepository), getSelf());

    } else {
      unhandled(message);
    }
  }
}
