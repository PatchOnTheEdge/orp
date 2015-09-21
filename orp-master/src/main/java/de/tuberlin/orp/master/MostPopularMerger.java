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
import de.tuberlin.orp.common.repositories.MostPopularRankingRepository;
import de.tuberlin.orp.common.rankings.RankingFilter;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class MostPopularMerger extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private ActorRef workerRouter;

  private RankingFilter filter;
  private MostPopularRankingRepository merger;


  public static Props create() {
    return Props.create(MostPopularMerger.class, new MostPopularMergerCreator());
  }

  @Override
  public void preStart() throws Exception {
    log.info("Merger started");

    merger = new MostPopularRankingRepository();
    filter = new RankingFilter();

    workerRouter = getContext().actorOf(FromConfig.getInstance().props(Props.empty()), "workerRouter");


    // asks every 2 seconds for the intermediate rankings
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(2, TimeUnit.SECONDS), () -> {

      merger.sortRankings();
      log.debug(merger.toString());
      workerRouter.tell(new Broadcast(new MergedRanking(merger, filter)), getSelf());
      merger = new MostPopularRankingRepository();

    }, getContext().dispatcher());

  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof WorkerResult) {

      // build cache and send it after timeout


      WorkerResult result = (WorkerResult) message;
      //log.info("Received intermediate rankings from " + getSender().toString());

      filter.merge(result.getFilter());
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
    private final MostPopularRankingRepository rankings;
    private final RankingFilter filter;

    public WorkerResult(MostPopularRankingRepository rankingRepository, RankingFilter filter) {
      this.rankings = rankingRepository;
      this.filter = filter;
    }

    public MostPopularRankingRepository getRankingRepository() {
      return rankings;
    }

    public RankingFilter getFilter() {
      return filter;
    }
  }

  private static class MostPopularMergerCreator implements Creator<MostPopularMerger> {
    @Override
    public MostPopularMerger create() throws Exception {
      return new MostPopularMerger();
    }
  }

  public static class MergedRanking implements Serializable {
    private MostPopularRankingRepository rankingRepository;
    private RankingFilter filter;

    public MergedRanking(MostPopularRankingRepository rankingRepository, RankingFilter filter) {
      this.rankingRepository = rankingRepository;
      this.filter = filter;
    }

    public MostPopularRankingRepository getRankingRepository() {
      return rankingRepository;
    }

    public RankingFilter getFilter() {
      return filter;
    }
  }
}
