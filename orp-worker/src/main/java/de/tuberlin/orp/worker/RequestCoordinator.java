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
import akka.actor.dsl.Creators;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.ranking.MostRecentRanking;
import de.tuberlin.orp.common.ranking.Ranking;
import de.tuberlin.orp.common.ranking.RankingFilter;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.message.OrpRequest;
import de.tuberlin.orp.common.repository.RankingRepository;
import de.tuberlin.orp.master.MostPopularMerger;
import de.tuberlin.orp.master.MostRecentMerger;
import de.tuberlin.orp.master.PopulaityMerger;
import de.tuberlin.orp.worker.algorithms.popularityTrend.PopularityWorker;
import scala.concurrent.Future;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

/**
 * This Actor coordinates requests.
 * All available algorithm-workers are asked for their mostPopularRanking.
 * The worker delegates which mostPopularRanking will be used.
 */
public class RequestCoordinator extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private ActorRef mostPopularWorker;
  private ActorRef mostRecentWorker;
  private ActorRef popularityWorker;
  private ActorRef filterActor;

  private RankingRepository mostPopularRanking;
  private RankingRepository mostRecentRanking;
  private RankingRepository trendRanking;

  private RankingFilter filter;

  public RequestCoordinator(ActorRef mostPopularWorker, ActorRef mostRecentWorker, ActorRef popularityWorker, ActorRef filterActor) {
    this.mostPopularWorker = mostPopularWorker;
    this.mostRecentWorker = mostRecentWorker;
    this.popularityWorker = popularityWorker;
    this.filterActor = filterActor;

    this.mostPopularRanking = new RankingRepository(new MostPopularRanking());
    this.mostRecentRanking = new RankingRepository(new MostRecentRanking());
    this.trendRanking = new RankingRepository(new MostPopularRanking());
    this.filter = new RankingFilter();
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpRequest) {

      OrpContext context = ((OrpRequest) message).getContext();
      String publisherId = context.getPublisherId();
      String userId = context.getUserId();

      log.info(String.format("Received request: publisherId = %s, userId = %s", publisherId, userId));

      Optional<Ranking> ranking = this.mostPopularRanking.getRanking(publisherId);
      ranking.ifPresent(ranking1 -> filter.filter(ranking1, context));

      getSender().tell(ranking.orElse(new MostPopularRanking()), getSelf());


      //TODO Handle removed items for all mergers
    } else if (message instanceof MostPopularMerger.MergedRanking) {

      // cache merged results
      MostPopularMerger.MergedRanking mergedRankingMessage = (MostPopularMerger.MergedRanking) message;
      mostPopularRanking = mergedRankingMessage.getRankingRepository();
      filter = mergedRankingMessage.getFilter();

      log.debug("Calculating intermediate ranking.");

      // calculate new intermediate results
      Future<Object> intermediateRankingFuture = Patterns.ask(mostPopularWorker, "getIntermediateRanking", 200);
      Future<Object> intermediateFilterFuture = Patterns.ask(filterActor, "getIntermediateFilter", 200);


      //Get Ranking from MostPopular Worker
      Future<MostPopularMerger.WorkerResult> workerResultFuture = getMostPopularWorkerResultFuture(intermediateRankingFuture, intermediateFilterFuture);
      Patterns.pipe(workerResultFuture, getContext().dispatcher()).to(getSender(), getSelf());

    } else if (message instanceof MostRecentMerger.MergedRanking) {

      MostRecentMerger.MergedRanking mergedRankingMessage = (MostRecentMerger.MergedRanking) message;
      mostRecentRanking = mergedRankingMessage.getRankingRepository();

      Future<Object> intermediateRankingFuture = Patterns.ask(mostRecentWorker, "getIntermediateRanking", 200);

      Future<MostRecentMerger.WorkerResult> workerResultFuture = getMostRecentWorkerResultFuture(intermediateRankingFuture);
      Patterns.pipe(workerResultFuture, getContext().dispatcher()).to(getSender(), getSelf());


    } else if (message instanceof PopulaityMerger.MergedRanking) {

      // cache merged results
      PopulaityMerger.MergedRanking mergedRankingMessage = (PopulaityMerger.MergedRanking) message;
      trendRanking = mergedRankingMessage.getRankingRepository();
      //filter = mergedRankingMessage.getFilter();

      log.debug("Calculating intermediate ranking.");

      // calculate new intermediate results
      Future<Object> intermediateRankingFuture = Patterns.ask(popularityWorker, "getIntermediateRanking", 200);
      Future<Object> intermediateFilterFuture = Patterns.ask(filterActor, "getIntermediateFilter", 200);

      //Get Ranking from MostPopular Worker
      Future<PopulaityMerger.WorkerResult> workerResultFuture = getPopularityWorkerResultFuture(intermediateRankingFuture, intermediateFilterFuture);
      Patterns.pipe(workerResultFuture, getContext().dispatcher()).to(getSender(), getSelf());

    } else {

      unhandled(message);

    }
  }
  private Future<MostPopularMerger.WorkerResult> getMostPopularWorkerResultFuture(Future<Object> intermediateRankingFuture, Future<Object> intermediateFilterFuture) {
    return Futures
        .sequence(Arrays.asList(intermediateRankingFuture, intermediateFilterFuture), getContext().dispatcher())
        .map(new Mapper<Iterable<Object>, MostPopularMerger.WorkerResult>() {
          @Override
          public MostPopularMerger.WorkerResult apply(Iterable<Object> parameter) {
            Iterator<Object> it = parameter.iterator();
            IntermediateRanking ranking = (IntermediateRanking) it.next();
            IntermediateFilter filter = (IntermediateFilter) it.next();
            log.debug(String.format("size(Ranking) = %s. size(Filter) = %s + %s",
                ranking.rankingRepository.getRankings().size(),
                filter.getFilter().getRecommended().size(), filter.getFilter().getRemoved().size()));
            log.debug(ranking.toString());
            return new MostPopularMerger.WorkerResult(
                ranking.getRankingRepository(),
                filter.getFilter());
          }
        }, getContext().dispatcher());
  }
  private Future<MostRecentMerger.WorkerResult> getMostRecentWorkerResultFuture(Future<Object> intermediateRankingFuture) {
    return Futures
            .sequence(Arrays.asList(intermediateRankingFuture), getContext().dispatcher())
            .map(new Mapper<Iterable<Object>, MostRecentMerger.WorkerResult>() {
              @Override
              public MostRecentMerger.WorkerResult apply(Iterable<Object> parameter) {
                Iterator<Object> it = parameter.iterator();
                IntermediateRanking ranking = (IntermediateRanking) it.next();
                log.debug(ranking.toString());
                return new MostRecentMerger.WorkerResult(
                    ranking.getRankingRepository());
              }
            }, getContext().dispatcher());
  }
  private Future<PopulaityMerger.WorkerResult> getPopularityWorkerResultFuture(Future<Object> intermediateRankingFuture, Future<Object> intermediateFilterFuture) {
    return Futures
        .sequence(Arrays.asList(intermediateRankingFuture, intermediateFilterFuture), getContext().dispatcher())
        .map(new Mapper<Iterable<Object>, PopulaityMerger.WorkerResult>() {
          @Override
          public PopulaityMerger.WorkerResult apply(Iterable<Object> parameter) {
            Iterator<Object> it = parameter.iterator();
            IntermediateRanking ranking = (IntermediateRanking) it.next();
            IntermediateFilter filter = (IntermediateFilter) it.next();
            log.debug(ranking.toString());
            return new PopulaityMerger.WorkerResult(
                ranking.getRankingRepository(),
                filter.getFilter());
          }
        }, getContext().dispatcher());
  }

  public static Props create(ActorRef mostPopularWorker, ActorRef mostRecentWorker, ActorRef popularityWorker, ActorRef filterActor) {
    return Props.create(RequestCoordinator.class, () -> {
      return new RequestCoordinator(mostPopularWorker, mostRecentWorker, popularityWorker, filterActor);
    });
  }

  public static class IntermediateRanking implements Serializable{
    private RankingRepository rankingRepository;

    public IntermediateRanking(RankingRepository rankingRepository) {
      this.rankingRepository = rankingRepository;
    }

    public RankingRepository getRankingRepository() {
      return rankingRepository;
    }
  }

  public static class IntermediateFilter implements Serializable{
    private RankingFilter filter;

    public IntermediateFilter(RankingFilter filter) {
      this.filter = filter;
    }

    public RankingFilter getFilter() {
      return filter;
    }
  }

}
