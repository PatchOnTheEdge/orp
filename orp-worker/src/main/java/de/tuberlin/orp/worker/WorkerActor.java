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
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.typesafe.config.Config;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.message.OrpNotification;
import de.tuberlin.orp.master.StatisticsActor;
import de.tuberlin.orp.master.MostPopularMerger;
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
  private ActorSelection mergerSelection;
  private ActorSelection filterSelection;
  private ActorSelection statisticsSelection;

  private long lastRanking = 0;
  private int receivedImpressions = 0;

  private HardwareAbstractionLayer hardware = new SystemInfo().getHardware();


  public static Props create() {
    return Props.create(WorkerActor.class, WorkerActor::new);
  }


  @Override
  public void preStart() throws Exception {
    super.preStart();
    Config config = getContext().system().settings().config();
    String master = config.getString("master");
    mergerSelection = getContext().actorSelection(master + "/user/merger");
    filterSelection = getContext().actorSelection(master + "/user/filter");
    statisticsSelection = getContext().actorSelection(master + "/user/statistics");
//    mergerSelection.tell(new Identify(0), getSelf());
    mostPopularWorker = getContext().actorOf(MostPopularWorker.create(500, 50), "mp");
    mergerSelection.tell(new MostPopularMerger.Register(mostPopularWorker), getSelf());
//    mostPopularMerger = getContext().actorOf(FromConfig.getInstance().props(MostPopularMergerOld.create()), "merger");

    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(1, TimeUnit.SECONDS), () -> {
      if (lastRanking != 0) {
        long now = System.currentTimeMillis();
        double throughput = 1000.0 * receivedImpressions / (now - lastRanking);

        double cpu = Arrays.stream(hardware.getProcessors())
            .mapToDouble(Processor::getSystemCpuLoadBetweenTicks)
            .sum();

        double memory = hardware.getMemory().getAvailable() / (double) hardware.getMemory().getTotal();


        StatisticsActor.WorkerStatistics statistics = new StatisticsActor.WorkerStatistics
            (throughput, cpu, memory, now);

        statisticsSelection.tell(statistics, getSelf());
      }

      lastRanking = System.currentTimeMillis();
      receivedImpressions = 0;
    }, getContext().dispatcher());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpNotification) {
      OrpNotification notification = (OrpNotification) message;

      String notificationType = notification.getType();

//      log.info(String.format("Received notification of type \"%s\"", notificationType));

      OrpContext context = notification.getContext();

      String publisher = context.getPublisherId();
      String itemId = context.getItemId();

      switch (notificationType) {
        case "event_notification":
//          log.info(String.format("Event Notification: Publisher = %s. Item ID = %s", publisher, itemId));

          ++receivedImpressions;

          if (!publisher.equals("") && !itemId.equals("") && !itemId.equals("0")) {
            mostPopularWorker.tell(context, getSelf());
          }

          break;

      }
    }
  }
}
