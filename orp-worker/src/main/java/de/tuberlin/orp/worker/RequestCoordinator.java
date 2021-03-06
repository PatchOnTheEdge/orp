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
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.message.OrpRequest;
import de.tuberlin.orp.common.ranking.*;
import de.tuberlin.orp.common.repository.RankingRepository;
import de.tuberlin.orp.master.*;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This Actor coordinates requests.
 * All available algorithm-workers are asked for their mostPopularRanking.
 * The worker delegates which mostPopularRanking will be used.
 */
public class RequestCoordinator extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private ActorRef mostPopularWorker;
  private ActorRef mostRecentWorker;
  private ActorRef popularCategoryWorker;
  private ActorRef filterActor;
  private ActorRef statisticAggregator;

  private RankingRepository mostPopularRanking;
  private RankingRepository mostRecentRanking;
  private RankingRepository popularCategoryRanking;

  private RankingFilter filter;

  private Map<String, Map<String, Integer>> clickStatistic;

  private File requestLogFile;
  private PrintWriter printWriter;


  public RequestCoordinator(ActorRef mostPopularWorker, ActorRef mostRecentWorker, ActorRef popularCategoryWorker, ActorRef filterActor, ActorRef statisticAggregator) {
    this.mostPopularWorker = mostPopularWorker;
    this.mostRecentWorker = mostRecentWorker;
    this.popularCategoryWorker = popularCategoryWorker;
    this.filterActor = filterActor;
    this.statisticAggregator = statisticAggregator;

    this.mostPopularRanking = new RankingRepository(new MostPopularRanking());
    this.mostRecentRanking = new RankingRepository(new MostRecentRanking());
    this.popularCategoryRanking = new RankingRepository(new PopularCategoryRanking());

    this.filter = new RankingFilter();

    this.clickStatistic = new HashMap<>();
//    this.requestLogFile = new File("log.txt");
  }

  @Override
  public void preStart() throws Exception {

//    this.printWriter = new PrintWriter(requestLogFile);

    log.info("Coordinator started. Writing Request Log at: " +  System.getProperty("user.dir"));

    getContext().system().scheduler().schedule(Duration.create(50, TimeUnit.SECONDS), Duration.create(10, TimeUnit.SECONDS), () -> {
      statisticAggregator.tell("getClickStatistic", getSelf());
    }, getContext().dispatcher());

  }
  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpRequest) {
      ActorRef sender = getSender();

      OrpContext context = ((OrpRequest) message).getContext();
      String publisherId = context.getPublisherId();
      String userId = context.getUserId();
      int limit = context.getLimit();

      log.debug(String.format("Received request: publisherId = %s, userId = %s", publisherId, userId));

      Map<String, Integer> clicks = clickStatistic.get(publisherId);
      String algorithmId = "mp";

      if (clicks != null){

        //Find Algorithms with Maximum Clicks and store them in Set
        int maxClick = Collections.max(clicks.values());
        List<String> maxClicked = clicks.entrySet().stream().filter(entry -> entry.getValue() == maxClick).map(Map.Entry::getKey).collect(Collectors.toList());

        //MP is used as the default Recommender
        if (!maxClicked.contains("mp")){
          //Pick one Algorithm randomly
          Collections.shuffle(maxClicked);
          algorithmId = maxClicked.get(0);
        }

//        printWriter.println(System.currentTimeMillis() + ";" + publisherId + ";" + algorithmId);
//        printWriter.flush();
      }

      switch (algorithmId){
        case "mp":
        default:
          Optional<Ranking> mpRanking = this.mostPopularRanking.getRanking(publisherId);
          mpRanking.ifPresent(ranking1 -> filter.filter(ranking1, context));
          sender.tell(mpRanking.orElse(new MostPopularRanking()), getSelf());
          break;
        case "mr":
          Optional<Ranking> mrRanking = this.mostRecentRanking.getRanking(publisherId);
          mrRanking.ifPresent(ranking1 -> filter.filter(ranking1, context));
          sender.tell(mrRanking.orElse(new MostRecentRanking()), getSelf());
          break;
        case "pc":
          Optional<Ranking> pcRanking = this.popularCategoryRanking.getRanking(publisherId);
          pcRanking.ifPresent(ranking1 -> filter.filter(ranking1, context));
          sender.tell(pcRanking.orElse(new PopularCategoryRanking()), getSelf());
          break;
      }

    } else if (message instanceof StatisticsManager.ClickStatistics) {

      this.clickStatistic = ((StatisticsManager.ClickStatistics) message).getClickStatistic();

    } else if (message instanceof MostPopularMerger.MergedRanking) {

      ActorRef sender = getSender();

      // cache merged results
      MostPopularMerger.MergedRanking mergedRankingMessage = (MostPopularMerger.MergedRanking) message;
      this.mostPopularRanking = mergedRankingMessage.getRankingRepository();

      // calculate new intermediate results
      Future<Object> intermediateRankingFuture = Patterns.ask(mostPopularWorker, "getIntermediateRanking", 200);

      //Get Ranking from MostPopular Worker
      Future<MostPopularMerger.WorkerResult> workerResultFuture = getMostPopularWorkerResultFuture(intermediateRankingFuture);
      Patterns.pipe(workerResultFuture, getContext().dispatcher()).to(sender, getSelf());


    } else if (message instanceof MostRecentMerger.MergedRanking) {

      ActorRef sender = getSender();

      // cache merged results
      MostRecentMerger.MergedRanking mergedRankingMessage = (MostRecentMerger.MergedRanking) message;
      this.mostRecentRanking = mergedRankingMessage.getRankingRepository();

      // calculate new intermediate results
      Future<Object> intermediateRankingFuture = Patterns.ask(mostRecentWorker, "getIntermediateRanking", 200);

      //Get Ranking from Worker
      Future<MostRecentMerger.WorkerResult> workerResultFuture = getMostRecentWorkerResultFuture(intermediateRankingFuture);
      Patterns.pipe(workerResultFuture, getContext().dispatcher()).to(sender, getSelf());

    } else if (message instanceof PopularCategoryMerger.MergedRanking) {

      ActorRef sender = getSender();

      // cache merged results
      PopularCategoryMerger.MergedRanking mergedRankingMessage = (PopularCategoryMerger.MergedRanking) message;
      this.popularCategoryRanking = mergedRankingMessage.getRankingRepository();

      // calculate new intermediate results
      Future<Object> intermediateRankingFuture = Patterns.ask(popularCategoryWorker, "getIntermediateRanking", 200);
//
//      //Get Ranking from Worker
      Future<PopularCategoryMerger.WorkerResult> workerResultFuture = getPopularCategoryWorkerResultFuture(intermediateRankingFuture);
      Patterns.pipe(workerResultFuture, getContext().dispatcher()).to(sender, getSelf());

    } else if (message instanceof FilterMerger.MergedFilter) {
      ActorRef sender = getSender();

      FilterMerger.MergedFilter mergedFilter = (FilterMerger.MergedFilter) message;
      filter = mergedFilter.getFilter();

//      Future<Object> intermediateFilterFuture = Patterns.ask(filterActor, "getIntermediateFilter", 200);
//      Future<FilterMerger.FilterResult> workerResultFuture = getFilterWorkerResultFuture(intermediateFilterFuture);
//      Patterns.pipe(workerResultFuture, getContext().dispatcher()).to(sender, getSelf());


    } else {
      unhandled(message);

    }
  }

  private Future<FilterMerger.FilterResult> getFilterWorkerResultFuture(Future<Object> intermediateFilterFuture) {
    return Futures
        .sequence(Arrays.asList(intermediateFilterFuture), getContext().dispatcher())
        .map(new Mapper<Iterable<Object>, FilterMerger.FilterResult>() {
          @Override
          public FilterMerger.FilterResult apply(Iterable<Object> parameter) {
            Iterator<Object> it = parameter.iterator();
            IntermediateFilter filter = (IntermediateFilter) it.next();

            return new FilterMerger.FilterResult(filter.getFilter());
          }
        }, getContext().dispatcher());
  }

  private Future<MostPopularMerger.WorkerResult> getMostPopularWorkerResultFuture(Future<Object> intermediateRankingFuture) {
    return Futures
        .sequence(Arrays.asList(intermediateRankingFuture), getContext().dispatcher())
        .map(new Mapper<Iterable<Object>, MostPopularMerger.WorkerResult>() {
          @Override
          public MostPopularMerger.WorkerResult apply(Iterable<Object> parameter) {
            Iterator<Object> it = parameter.iterator();
            IntermediateRanking ranking = (IntermediateRanking) it.next();

            return new MostPopularMerger.WorkerResult(ranking.getRankingRepository());
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

                return new MostRecentMerger.WorkerResult(ranking.getRankingRepository());
              }
            }, getContext().dispatcher());
  }
  private Future<PopularCategoryMerger.WorkerResult> getPopularCategoryWorkerResultFuture(Future<Object> intermediateRankingFuture) {
    return Futures
        .sequence(Arrays.asList(intermediateRankingFuture), getContext().dispatcher())
        .map(new Mapper<Iterable<Object>, PopularCategoryMerger.WorkerResult>() {
          @Override
          public PopularCategoryMerger.WorkerResult apply(Iterable<Object> parameter) {
            Iterator<Object> it = parameter.iterator();
            IntermediateRanking ranking = (IntermediateRanking) it.next();

            return new PopularCategoryMerger.WorkerResult(ranking.getRankingRepository());
          }
        }, getContext().dispatcher());
  }


  public static Props create(ActorRef mostPopularWorker, ActorRef mostRecentWorker, ActorRef popularCategoryWorker, ActorRef filterActor, ActorRef statisticAggregator) {
    return Props.create(RequestCoordinator.class, () -> {
      return new RequestCoordinator(mostPopularWorker, mostRecentWorker , popularCategoryWorker, filterActor, statisticAggregator);
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
