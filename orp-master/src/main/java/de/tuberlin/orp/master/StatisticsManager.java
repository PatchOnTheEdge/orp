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
import akka.japi.Creator;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BinaryOperator;

public class StatisticsManager extends UntypedActor {
  private LinkedHashMap<ActorRef, ArrayDeque<WorkerStatistics>> workerStatistics;

  private int maxSize = 1000;

  public static Props create() {
    return Props.create(StatisticsManager.class, new CentralStatisticsActorCreator());
  }

  public StatisticsManager() {
    workerStatistics = new LinkedHashMap<>();
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

    public WorkerStatistics(long timestamp, double throughput, Map<Short, Long> responseTimes,
                            long requestCounter, long notificationCounter, long clickCounter) {
      this.timestamp = timestamp;
      this.throughput = throughput;
      this.responseTimes = responseTimes;
      this.requestCounter = requestCounter;
      this.notificationCounter = notificationCounter;
      this.clickCounter = clickCounter;
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

    } else {
      unhandled(message);
    }
  }

  private static class CentralStatisticsActorCreator implements Creator<StatisticsManager> {
    @Override
    public StatisticsManager create() throws Exception {
      return new StatisticsManager();
    }
  }
}
