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

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class CentralStatisticsActor extends UntypedActor {
  private HashMap<ActorRef, ArrayDeque<WorkerStatistics>> workerStatistics;
  private int maxSize = 1000;

  public static Props create() {
    return Props.create(CentralStatisticsActor.class, new CentralStatisticsActorCreator());
  }

  public CentralStatisticsActor() {
    workerStatistics = new HashMap<>();
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
  }


  public static class RetrieveStatistics implements Serializable {

  }

  public static class StatisticsMessage implements Serializable {
    private LinkedHashMap<ActorRef, WorkerStatistics> statistics;

    public StatisticsMessage(LinkedHashMap<ActorRef, WorkerStatistics> statistics) {
      this.statistics = statistics;
    }

    public LinkedHashMap<ActorRef, WorkerStatistics> getStatistics() {
      return statistics;
    }
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

    } else if (message instanceof RetrieveStatistics) {

      LinkedHashMap<ActorRef, WorkerStatistics> currentStats = new LinkedHashMap<>();
      for (ActorRef actorRef : workerStatistics.keySet()) {
        currentStats.put(actorRef, workerStatistics.get(actorRef).getFirst());
      }

      getSender().tell(new StatisticsMessage(currentStats), getSelf());
    }
  }

  private static class CentralStatisticsActorCreator implements Creator<CentralStatisticsActor> {
    @Override
    public CentralStatisticsActor create() throws Exception {
      return new CentralStatisticsActor();
    }
  }
}
