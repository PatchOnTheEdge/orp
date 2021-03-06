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

package de.tuberlin.orp.worker.algorithms.mostPopular;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.repository.RankingRepository;
import de.tuberlin.orp.worker.RequestCoordinator;

/**
 * An actor that can receive items and store them per publisher in a private Hashmap. The top n entries will be send to
 * the MostPopularMerger in a defined interval.
 */
public class MostPopularWorker extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private OrpContextCounter2 contextCounter;

  static class MostPopularActorCreator implements Creator<MostPopularWorker> {

    private int contextWindowSize;
    private int topListSize;

    public MostPopularActorCreator(int contextWindowSize, int topListSize) {
      this.contextWindowSize = contextWindowSize;
      this.topListSize = topListSize;
    }

    @Override
    public MostPopularWorker create() throws Exception {
      return new MostPopularWorker(contextWindowSize, topListSize);
    }
  }

  /**
   * @see #MostPopularWorker(int, int)
   */
  public static Props create(int contextWindowSize, int topListSize) {
    return Props.create(MostPopularWorker.class, new MostPopularActorCreator(contextWindowSize, topListSize));
  }

  /**
   * @param contextWindowSize
   *     The window size which defines of how many contexts a window should consist.
   * @param topListSize
   *     The maximum size of the ranking for the publishers.
   */
  public MostPopularWorker(int contextWindowSize, int topListSize) {
    this.contextCounter = new OrpContextCounter2(contextWindowSize, topListSize);
  }

  @Override
  public void preStart() throws Exception {
    log.info("Most Popular Worker started.");
    super.preStart();
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpContext) {

      OrpContext context = (OrpContext) message;
      log.debug(String.format("Received OrpArticle from Publisher: %s with ID: %s", context.getPublisherId(), context.getItemId()));
      contextCounter.add(context);

    } else if (message.equals("getIntermediateRanking")) {

      log.debug("Intermediate ranking requested.");
      RankingRepository rankingRepository = contextCounter.getRankingRepository();
      getSender().tell(new RequestCoordinator.IntermediateRanking(rankingRepository), getSelf());

    } else {
      unhandled(message);
    }
  }
}
