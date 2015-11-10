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
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.ranking.Ranking;
import de.tuberlin.orp.common.repository.MostPopularRankingRepository;
import de.tuberlin.orp.common.repository.RankingRepository;
import de.tuberlin.orp.common.ranking.RankingFilter;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.message.OrpRequest;
import de.tuberlin.orp.master.MostPopularMerger;
import scala.concurrent.Future;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;

/**
 * This Actor coordinates requests.
 * All available algorithm-workers are asked for their ranking.
 * The worker delegates which ranking will be used.
 */
public class RequestCoordinator extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private ActorRef mostPopularWorker;
  private ActorRef mostRecentWorker;
  private ActorRef filterActor;

  private RankingRepository ranking;
  private RankingFilter filter;

  public RequestCoordinator(ActorRef mostPopularWorker, ActorRef mostRecentWorker, ActorRef filterActor) {
    this.mostPopularWorker = mostPopularWorker;
    this.mostRecentWorker = mostRecentWorker;
    this.filterActor = filterActor;

    this.ranking = new MostPopularRankingRepository();
    this.filter = new RankingFilter();
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpRequest) {

      OrpContext context = ((OrpRequest) message).getContext();
      String publisherId = context.getPublisherId();
      String userId = context.getUserId();

      log.info(String.format("Received request: publisherId = %s, userId = %s", publisherId, userId));


      Optional<Ranking<MostPopularRanking>> ranking = this.ranking.getRanking(publisherId);
      ranking.ifPresent(ranking1 -> filter.filter(ranking1, context));

      getSender().tell(ranking.orElse(new MostPopularRanking()), getSelf());

    } else if (message instanceof MostPopularMerger.MergedRanking) {

      // cache merged results
      MostPopularMerger.MergedRanking mergedRankingMessage = (MostPopularMerger.MergedRanking) message;
      ranking = mergedRankingMessage.getRankingRepository();
      filter = mergedRankingMessage.getFilter();

      //log.info("Calculating intermediate ranking.");

      // calculate new intermediate results
      Future<Object> intermediateRankingFuture = Patterns.ask(mostPopularWorker, "getIntermediateRanking", 200);
      Future<Object> intermediateFilterFuture = Patterns.ask(filterActor, "getIntermediateFilter", 200);

      //TODO merger result to big...
      Future<MostPopularMerger.WorkerResult> workerResultFuture = Futures
          .sequence(Arrays.asList(intermediateRankingFuture, intermediateFilterFuture), getContext().dispatcher())
          .map(new Mapper<Iterable<Object>, MostPopularMerger.WorkerResult>() {
            @Override
            public MostPopularMerger.WorkerResult apply(Iterable<Object> parameter) {
              Iterator<Object> it = parameter.iterator();
              IntermediateRanking ranking = (IntermediateRanking) it.next();
              IntermediateFilter filter = (IntermediateFilter) it.next();
              log.debug(ranking.toString());
              return new MostPopularMerger.WorkerResult(
                  ranking.getRankingRepository(),
                  filter.getFilter());
            }
          }, getContext().dispatcher());

      Patterns.pipe(workerResultFuture, getContext().dispatcher()).to(getSender());

    } else {

      unhandled(message);

    }
  }

  public static Props create(ActorRef mostPopularWorker, ActorRef mostRecentWorker, ActorRef filterActor) {
    return Props.create(RequestCoordinator.class, () -> {
      return new RequestCoordinator(mostPopularWorker, mostRecentWorker, filterActor);
    });
  }

  public static class IntermediateRanking {
    private RankingRepository rankingRepository;

    public IntermediateRanking(RankingRepository rankingRepository) {
      this.rankingRepository = rankingRepository;
    }

    public RankingRepository getRankingRepository() {
      return rankingRepository;
    }
  }

  public static class IntermediateFilter {
    private RankingFilter filter;

    public IntermediateFilter(RankingFilter filter) {
      this.filter = filter;
    }

    public RankingFilter getFilter() {
      return filter;
    }
  }

}
