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

package de.tuberlin.orp.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import akka.routing.Broadcast;
import akka.routing.FromConfig;
import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.ranking.RankingFilter;
import de.tuberlin.orp.common.repository.RankingRepository;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class PopulaityMerger extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private ActorRef workerRouter;

  private RankingRepository merger;


  public static Props create() {
    return Props.create(PopulaityMerger.class, new PopularityMergerCreator());
  }

  @Override
  public void preStart() throws Exception {
    log.info("Merger started");

    merger = new RankingRepository(new MostPopularRanking());

    workerRouter = getContext().actorOf(FromConfig.getInstance().props(Props.empty()), "workerRouter");


    // asks every 2 seconds for the intermediate ranking
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(2, TimeUnit.SECONDS), () -> {

      merger.sortRankings();
      log.debug(merger.toString());
      workerRouter.tell(new Broadcast(new MergedRanking(merger)), getSelf());
      merger = new RankingRepository(new MostPopularRanking());

    }, getContext().dispatcher());

  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof PopulaityMerger.WorkerResult) {

      // build cache and send it after timeout

      WorkerResult result = (WorkerResult) message;
      log.debug("Received intermediate ranking from " + getSender().toString());

      merger.merge(result.getRankingRepository());

    }
    else if (message.equals("getMergerResult")){
      getSender().tell(this.merger.getRankings(),getSelf());
    }
    else {
      unhandled(message);
    }
  }

  public static class WorkerResult implements Serializable {
    private final RankingRepository rankings;

    public WorkerResult(RankingRepository rankingRepository) {
      this.rankings = rankingRepository;
    }

    public RankingRepository getRankingRepository() {
      return rankings;
    }
  }

  private static class PopularityMergerCreator implements Creator<PopulaityMerger> {
    @Override
    public PopulaityMerger create() throws Exception {
      return new PopulaityMerger();
    }
  }

  public static class MergedRanking implements Serializable {
    private RankingRepository rankingRepository;
    private RankingFilter filter;

    public MergedRanking(RankingRepository rankingRepository) {
      this.rankingRepository = rankingRepository;
    }

    public RankingRepository getRankingRepository() {
      return rankingRepository;
    }

    public RankingFilter getFilter() {
      return filter;
    }
  }
}
