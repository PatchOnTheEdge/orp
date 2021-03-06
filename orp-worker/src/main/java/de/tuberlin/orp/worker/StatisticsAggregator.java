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

import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.tuberlin.orp.master.StatisticsManager;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StatisticsAggregator extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private long requestCounter;
  private long notificationCounter;
  private long clickCounter;

  private Map<String, Set<String>> clickEvents;
  private Map<Short, Long> responseTimes;
  private Map<String, Map<String, Integer>> clickStatistics;
  private ActorSelection statisticsManager;

  public static class ResponseTime implements Serializable {
    private long responseTime;

    public ResponseTime(long responseTime) {
      this.responseTime = responseTime;
    }

    public long getResponseTime() {
      return responseTime;
    }
  }

  public static Props create(ActorSelection statisticsManager) {
    return Props.create(StatisticsAggregator.class, () -> {
      return new StatisticsAggregator(statisticsManager);
    });
  }

  public StatisticsAggregator(ActorSelection statisticsManager) {
    this.statisticsManager = statisticsManager;
    clickEvents = new HashMap<>();
    responseTimes = new HashMap<>();
    clickStatistics = new HashMap<>();
  }

  @Override
  public void preStart() throws Exception {
    log.info("Statistics Aggregator started.");
    FiniteDuration aggregationInterval = Duration.create(1, TimeUnit.SECONDS);
    getContext().system().scheduler().schedule(Duration.Zero(), aggregationInterval, () -> {

      double throughput = requestCounter / (double) aggregationInterval.toSeconds();
      statisticsManager.tell(new StatisticsManager.WorkerStatistics(System.currentTimeMillis(),
          throughput, responseTimes, requestCounter, notificationCounter, clickCounter, clickEvents), getSelf());
      requestCounter = 0;
      notificationCounter = 0;
      clickCounter = 0;
      responseTimes = new HashMap<>();
      clickEvents = new HashMap<>();

    }, getContext().dispatcher());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof ResponseTime) {

      short responseTime = (short) ((ResponseTime) message).getResponseTime();
      responseTimes.merge(responseTime, 1L, Long::sum);

    } else if (message.equals("request")) {

      ++requestCounter;
//      log.info("Interval Request Counter: " + requestCounter);

    } else if (message.equals("notification")){

      ++notificationCounter;

    } else if (message instanceof ClickEvent){

      ++clickCounter;

      ClickEvent clickEvent = (ClickEvent) message;
      Set<String> clickedItems = clickEvents.getOrDefault(clickEvent.getPublisherId(), new HashSet<>());
      clickedItems.add(clickEvent.getItemId());
      clickEvents.put(clickEvent.getPublisherId(), clickedItems);

    } else if (message instanceof StatisticsManager.ClickStatistics){

      this.clickStatistics = ((StatisticsManager.ClickStatistics) message).getClickStatistic();

    } else if (message.equals("getClickStatistic")){

      getSender().tell(new StatisticsManager.ClickStatistics(this.clickStatistics), getSelf());

    } else {
      unhandled(message);
    }
  }
  public static class ClickEvent implements Serializable{
    private String publisherId;
    private String itemId;
    public ClickEvent(String publisherId, String itemId) {
      this.publisherId = publisherId;
      this.itemId = itemId;
    }

    public String getPublisherId() {
      return publisherId;
    }

    public String getItemId() {
      return itemId;
    }
  }

}
