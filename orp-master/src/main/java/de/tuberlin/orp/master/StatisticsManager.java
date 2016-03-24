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
import akka.dispatch.Mapper;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.ranking.MostRecentRanking;
import io.verbit.ski.core.http.result.Result;
import io.verbit.ski.core.json.Json;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;

import static io.verbit.ski.core.http.result.SimpleResult.ok;

public class StatisticsManager extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private LinkedHashMap<ActorRef, ArrayDeque<WorkerStatistics>> workerStatistics;
  private Map<String, Set<String>> mostPopularRecommendations;
  private Map<String, Set<String>> mostRecentRecommendations;
  private ActorRef mostPopularMerger;
  private ActorRef mostRecentMerger;
  private Map<String, Integer> algorithmClicks;
  private int maxSize = 1000;

  public StatisticsManager(ActorRef mostPopularMerger, ActorRef mostRecentMerger) {
    workerStatistics = new LinkedHashMap<>();
    mostPopularRecommendations = new HashMap<>();
    mostRecentRecommendations = new HashMap<>();
    algorithmClicks = new HashMap<>();
//    mostRecentMerger = getContext().actorOf(FromConfig.getInstance().props(Props.empty()), "recentMerger");
//    mostPopularMerger = getContext().actorOf(FromConfig.getInstance().props(Props.empty()), "popularMerger");
    this.mostPopularMerger = mostPopularMerger;
    this.mostRecentMerger = mostRecentMerger;
  }

  @Override
  public void preStart() throws Exception {
    log.info("Statistics Manager started.");

    // asks every 30 seconds for the Most Popular Ranking
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(30, TimeUnit.SECONDS), this::getMostPopularMergerResult, getContext().dispatcher());

    // asks every 30 seconds for the Most Recent Ranking
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(30, TimeUnit.SECONDS), this::getMostRecentMergerResult, getContext().dispatcher());

    //Calculate the number of clicked Recommendations for each Ranking, every 30 Seconds
    getContext().system().scheduler().schedule(Duration.create(10, TimeUnit.SECONDS), Duration.create(30, TimeUnit.SECONDS), () -> {

      Integer mostPopularClicks =  0;
      Integer mostRecentClicks = 0;

      long start = System.currentTimeMillis();

      for (Map.Entry<ActorRef, ArrayDeque<WorkerStatistics>> entry : workerStatistics.entrySet()) {

        for (WorkerStatistics statistic : entry.getValue()) {
          for (Set<String> clickedItems : statistic.getClickEvents().values()) {
            for (Set<String> items : mostPopularRecommendations.values()) {
              items.retainAll(clickedItems);
              mostPopularClicks += items.size();
            }
            for (Set<String> items : mostRecentRecommendations.values()) {
              items.retainAll(clickedItems);
              mostRecentClicks += items.size();
            }
          }
        }
      }
      algorithmClicks.put("MostPopular", mostPopularClicks );
      algorithmClicks.put("MostRecent", mostRecentClicks);
//      log.info("Mp clicks = " + mostPopularClicks + ". MR clicks = " + mostRecentClicks + ". Calculation Time = " + (System.currentTimeMillis() - start) );

    }, getContext().dispatcher());

  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof WorkerStatistics) {

      workerStatistics.putIfAbsent(getSender(), new ArrayDeque<>());
      ArrayDeque<WorkerStatistics> statistics = workerStatistics.get(getSender());

      WorkerStatistics workerStatistics = (WorkerStatistics) message;

      statistics.addFirst(workerStatistics);
      if (statistics.size() > maxSize) {
        statistics.removeLast();
      }


    } else if (message.equals("getCurrentStatistics")) {

      LinkedHashMap<ActorRef, WorkerStatistics> currentWorkerStats = new LinkedHashMap<>();
      for (ActorRef actorRef : workerStatistics.keySet()) {
        currentWorkerStats.put(actorRef, workerStatistics.get(actorRef).getFirst());
      }

      getSender().tell(new StatisticsMessage(currentWorkerStats), getSelf());


    } else if (message.equals("getStatisticsReport")) {

      BinaryOperator<Map<Short, Long>> histogramMerger = (hist1, hist2) -> {
        Map<Short, Long> result = new HashMap<>(hist1);
        hist2.forEach((responseTime, count) -> result.merge(responseTime, count, Long::sum));
        return result;
      };

      Map<Short, Long> responseTimes = workerStatistics.values().stream()
          .map(workerStatistics -> workerStatistics.stream()
              .map(WorkerStatistics::getResponseTimes)
              .reduce(new HashMap<>(), histogramMerger))
          .reduce(new HashMap<>(), histogramMerger);

      getSender().tell(new StatisticsReport(workerStatistics, responseTimes), getSelf());


    } else if (message.equals("getClicks")) {

      getSender().tell(this.algorithmClicks, getSelf());


    } else {
      unhandled(message);
    }
  }

  private Future<Result> getMostPopularMergerResult() {
    return Patterns.ask(mostPopularMerger, "getMergerResult", 100)
        .map(new Mapper<Object, Result>() {

          @Override
          public Result apply(Object object) {
            Map<String, MostPopularRanking> pubRankMap = (Map<String, MostPopularRanking>) object;

            for (String publisher : pubRankMap.keySet()) {
              MostPopularRanking mostPopularRanking = pubRankMap.get(publisher);
              StatisticsManager.this.mostPopularRecommendations.put(publisher, mostPopularRanking.getRanking().keySet());
            }
            return ok(Json.newObject());
          }
        }, getContext().dispatcher());
  }

  private Future<Result> getMostRecentMergerResult() {
    return Patterns.ask(mostRecentMerger, "getMergerResult", 100)
        .map(new Mapper<Object, Result>() {

          @Override
          public Result apply(Object object) {
            Map<String, MostRecentRanking> pubRankMap = (Map<String, MostRecentRanking>) object;

            for (String publisher : pubRankMap.keySet()) {
              MostRecentRanking ranking = pubRankMap.get(publisher);
              StatisticsManager.this.mostRecentRecommendations.put(publisher, ranking.getRanking().keySet());
            }
            return ok(Json.newObject());
          }
        }, getContext().dispatcher());
  }

  public static Props create(ActorRef mostPopularMerger, ActorRef mostRecentMerger) {
    return Props.create(StatisticsManager.class, () -> {
      return new StatisticsManager(mostPopularMerger, mostRecentMerger);
    });
  }

  public static class StatisticsReport implements Serializable {
    private LinkedHashMap<ActorRef, ArrayDeque<WorkerStatistics>> workerStatistics;
    private SortedMap<Short, Long> responseTimes;

    public StatisticsReport(LinkedHashMap<ActorRef, ArrayDeque<WorkerStatistics>> workerStatistics,
                            Map<Short, Long> responseTimes) {
      this.workerStatistics = workerStatistics;
      this.responseTimes = new TreeMap<>(responseTimes);
    }

    public LinkedHashMap<ActorRef, ArrayDeque<WorkerStatistics>> getWorkerStatistics() {
      return workerStatistics;
    }

    public SortedMap<Short, Long> getResponseTimes() {
      return responseTimes;
    }
  }

  public static class StatisticsMessage implements Serializable {
    private LinkedHashMap<ActorRef, WorkerStatistics> workerStatistics;

    public StatisticsMessage(LinkedHashMap<ActorRef, WorkerStatistics> workerStatistics) {
      this.workerStatistics = workerStatistics;
    }

    public LinkedHashMap<ActorRef, WorkerStatistics> getWorkerStatistics() {
      return workerStatistics;
    }
  }

  public static class WorkerStatistics implements Serializable {

    private long timestamp;
    private double throughput;
    private Map<Short, Long> responseTimes;
    private long requestCounter;
    private long notificationCounter;
    private long clickCounter;
    private Map<String, Set<String>> clickEvents;

    public WorkerStatistics(long timestamp, double throughput, Map<Short, Long> responseTimes,
                            long requestCounter, long notificationCounter, long clickCounter, Map<String, Set<String>> clickEvents) {
      this.timestamp = timestamp;
      this.throughput = throughput;
      this.responseTimes = responseTimes;
      this.requestCounter = requestCounter;
      this.notificationCounter = notificationCounter;
      this.clickCounter = clickCounter;
      this.clickEvents = clickEvents;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public double getThroughput() {
      return throughput;
    }

    public Map<Short, Long> getResponseTimes() {
      return responseTimes;
    }

    public long getRequestCounter() {
      return requestCounter;
    }

    public long getNotificationCounter() {
      return notificationCounter;
    }

    public long getClickCounter() {
      return clickCounter;
    }

    public Map<String, Set<String>> getClickEvents() {
      return clickEvents;
    }
  }
}
