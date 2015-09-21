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
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.tuberlin.orp.common.messages.OrpContext;
import de.tuberlin.orp.common.messages.OrpItemUpdate;
import de.tuberlin.orp.common.messages.OrpNotification;
import de.tuberlin.orp.common.messages.OrpRequest;
import de.tuberlin.orp.worker.algorithms.popular.MostPopularWorker;
import de.tuberlin.orp.worker.algorithms.recent.MostRecentWorker;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.Processor;
import scala.concurrent.duration.Duration;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * This actor is the entry point for the Akka application. All Requests received over HTTP are transformed to Akka
 * messages and sent to this actor.
 */
public class WorkerActor extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private ActorRef mostPopularWorker;
  private ActorRef mostRecentWorker;
  private ActorRef filterActor;
  private ActorRef statisticsAggregator;
  private ActorRef requestCoordinator;

  private HardwareAbstractionLayer hardware = new SystemInfo().getHardware();

  public WorkerActor(ActorRef statisticsAggregator) {
    this.statisticsAggregator = statisticsAggregator;
  }


  public static Props create(ActorRef statisticsAggregator) {
    return Props.create(WorkerActor.class, () -> {
      return new WorkerActor(statisticsAggregator);
    });
  }


  @Override
  public void preStart() throws Exception {
    super.preStart();

    mostPopularWorker = getContext().actorOf(MostPopularWorker.create(500, 50), "mp");
    mostRecentWorker = getContext().actorOf(MostRecentWorker.create(50), "mr");

    filterActor = getContext().actorOf(RecommendationFilter.create(), "filter");

    requestCoordinator = getContext().actorOf(RequestCoordinator.create(mostPopularWorker, mostRecentWorker,filterActor), "coordinator");

    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(1, TimeUnit.SECONDS), () -> {

      double cpu = Arrays.stream(hardware.getProcessors())
          .mapToDouble(Processor::getSystemCpuLoadBetweenTicks)
          .sum();

      double memory = hardware.getMemory().getAvailable() / (double) hardware.getMemory().getTotal();

    }, getContext().dispatcher());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpNotification) {
      OrpNotification notification = (OrpNotification) message;

      String notificationType = notification.getType();

      log.info(String.format("Received notification of type \"%s\"", notificationType));

      OrpContext context = notification.getContext();

      String publisherId = context.getPublisherId();
      String itemId = context.getItemId();

      switch (notificationType) {
        case "event_notification":
          log.info(String.format("Received event notification: publisherId = %s. itemId = %s", publisherId, itemId));

          statisticsAggregator.tell("request", getSelf());

          if (!publisherId.equals("") && !itemId.equals("") && !itemId.equals("0")) {
            mostPopularWorker.tell(context, getSelf());

            filterActor.tell(new RecommendationFilter.Clicked(context.getUserId(), context.getItemId()), getSelf());
          }

          break;

      }
    } else if (message instanceof OrpRequest) {

      // requests are handled by the coordinator
      requestCoordinator.forward(message, getContext());

    } else if (message instanceof OrpItemUpdate) {

      // look for non recommendable items
      OrpItemUpdate itemUpdate = (OrpItemUpdate) message;
      if (!itemUpdate.isItemRecommendable()) {
        filterActor.tell(new RecommendationFilter.Removed(itemUpdate.getItemId()), getSelf());
      }

    } else {
      unhandled(message);
    }
  }
}
