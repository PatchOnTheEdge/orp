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

package de.tuberlin.orp.merger;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;

public class CentralStatisticsActor extends UntypedActor {
  private LinkedHashMap<ActorRef, ArrayDeque<WorkerStatistics>> workerStatistics;
  private ArrayDeque<MergerStatistics> responseTimes;
  private ArrayDeque<Long> responseTimesAggregator;

  private int maxSize = 1000;

  public static Props create() {
    return Props.create(CentralStatisticsActor.class, new CentralStatisticsActorCreator());
  }

  public CentralStatisticsActor() {
    workerStatistics = new LinkedHashMap<>();
    responseTimes = new ArrayDeque<>();
    responseTimesAggregator = new ArrayDeque<>();
  }

  public static class WorkerStatistics implements Serializable {
    private double throughput;
    private double cpu;
    private double memory;
    private long timestamp;

    public WorkerStatistics(double throughput, double cpu, double memory, long timestamp) {
      this.throughput = throughput;
      this.cpu = cpu;
      this.memory = memory;
      this.timestamp = timestamp;
    }

    public double getThroughput() {
      return throughput;
    }

    public double getCpu() {
      return cpu;
    }

    public double getMemory() {
      return memory;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }

  public static class MergerStatistics implements Serializable {
    private double throughput;
    private double cpu;
    private double memory;
    private long timestamp;
    private long responseTime;


    public MergerStatistics(long timestamp, long responseTime) {
      this.timestamp = timestamp;
      this.responseTime = responseTime;
    }

    public MergerStatistics(long responseTime) {
      this.responseTime = responseTime;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public long getResponseTime() {
      return responseTime;
    }
  }

  public static class RetrieveStatistics implements Serializable {
  }

  public static class RetrieveStatisticsReport implements Serializable {
  }

  public static class StatisticsReport implements Serializable {
    private LinkedHashMap<ActorRef, ArrayDeque<WorkerStatistics>> workerStatistics;
    private ArrayDeque<MergerStatistics> responseTimes;

    public StatisticsReport(LinkedHashMap<ActorRef, ArrayDeque<WorkerStatistics>> workerStatistics,
        ArrayDeque<MergerStatistics> responseTimes) {
      this.workerStatistics = workerStatistics;
      this.responseTimes = responseTimes;
    }

    public LinkedHashMap<ActorRef, ArrayDeque<WorkerStatistics>> getWorkerStatistics() {
      return workerStatistics;
    }

    public ArrayDeque<MergerStatistics> getResponseTimes() {
      return responseTimes;
    }
  }



  public static class StatisticsMessage implements Serializable {
    private LinkedHashMap<ActorRef, WorkerStatistics> workerStatistics;
    private MergerStatistics responseTimeStatistics;

    public StatisticsMessage(LinkedHashMap<ActorRef, WorkerStatistics> workerStatistics,
        MergerStatistics responseTimeStatistics) {
      this.workerStatistics = workerStatistics;
      this.responseTimeStatistics = responseTimeStatistics;
    }

    public LinkedHashMap<ActorRef, WorkerStatistics> getWorkerStatistics() {
      return workerStatistics;
    }

    public MergerStatistics getResponseTimeStatistics() {
      return responseTimeStatistics;
    }
  }


  @Override
  public void preStart() throws Exception {
    super.preStart();
    // aggregate response times
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(1, TimeUnit.SECONDS), () -> {

      OptionalDouble optAvg = responseTimesAggregator.stream().mapToLong(d -> d).average();
      if (optAvg.isPresent()) {
        long average = (long) optAvg.getAsDouble();
        responseTimesAggregator.clear();

        responseTimes.addFirst(new MergerStatistics(System.currentTimeMillis(), average));
        if (responseTimes.size() > maxSize) {
          responseTimes.removeLast();
        }
      }
    }, getContext().dispatcher());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof WorkerStatistics) {

      workerStatistics.putIfAbsent(getSender(), new ArrayDeque<>());
      ArrayDeque<WorkerStatistics> statistics = workerStatistics.get(getSender());

      statistics.addFirst((WorkerStatistics) message);
      if (statistics.size() > maxSize) {
        statistics.removeLast();
      }
    } else if (message instanceof MergerStatistics) {

      responseTimesAggregator.addFirst(((MergerStatistics) message).getResponseTime());

    } else if (message instanceof RetrieveStatistics) {

      LinkedHashMap<ActorRef, WorkerStatistics> currentWorkerStats = new LinkedHashMap<>();
      for (ActorRef actorRef : workerStatistics.keySet()) {
        currentWorkerStats.put(actorRef, workerStatistics.get(actorRef).getFirst());
      }


      MergerStatistics currentResponseTimeStats = null;
      if (!responseTimes.isEmpty()) {
        currentResponseTimeStats = responseTimes.getFirst();
      }

      getSender().tell(new StatisticsMessage(currentWorkerStats, currentResponseTimeStats), getSelf());

    } else if (message instanceof RetrieveStatisticsReport) {

      getSender().tell(new StatisticsReport(workerStatistics, responseTimes), getSelf());

    }
  }

  private static class CentralStatisticsActorCreator implements Creator<CentralStatisticsActor> {
    @Override
    public CentralStatisticsActor create() throws Exception {
      return new CentralStatisticsActor();
    }
  }
}
