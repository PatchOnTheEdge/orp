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

package de.tuberlin.orp.akka.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.RoundRobinPool;
import de.tuberlin.orp.core.Context;

/**
 * This actor is the entry point for the Akka application. All Requests received over HTTP are transformed to Akka
 * messages and sent to this actor.
 */
public class CentralOrpActor extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private ActorRef mostPopularMerger;
  private final int noOfWorkers;
  private ActorRef mostPopularWorker;


  public static Props props(int noOfWorkers) {
    return Props.create(CentralOrpActor.class, () -> {
      return new CentralOrpActor(noOfWorkers);
    });
  }

  public CentralOrpActor(int noOfWorkers) {
    this.noOfWorkers = noOfWorkers;
  }


  public static class OrpNotification {
    private String type;
    private Context context;

    public OrpNotification() {
    }

    public OrpNotification(String type, Context context) {
      this.type = type;
      this.context = context;
    }

    public String getType() {
      return type;
    }

    public Context getContext() {
      return context;
    }
  }

  public static class OrpItemUpdate {
    private String itemId;
    private int flag;

    public OrpItemUpdate() {
    }

    public OrpItemUpdate(String itemId, int flag) {
      this.itemId = itemId;
      this.flag = flag;
    }

    public String getItemId() {
      return itemId;
    }

    public boolean isItemRecommendable() {
      return (flag & 1) == 1;
    }
  }

  public static class OrpRequest {
    private Context context;

    public OrpRequest() {
    }

    public OrpRequest(Context context) {
      this.context = context;
    }

    public Context getContext() {
      return context;
    }
  }


  @Override
  public void preStart() throws Exception {
    super.preStart();
    mostPopularMerger = getContext().actorOf(MostPopularMerger.create(), "merger");
    mostPopularWorker = getContext().actorOf(
        MostPopularActor.create(mostPopularMerger, 500, 50).withRouter(new RoundRobinPool(noOfWorkers)), "mp");
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpNotification) {
      OrpNotification notification = (OrpNotification) message;

      String notificationType = notification.getType();

//      log.info(String.format("Received notification of type \"%s\"", notificationType));

      Context context = notification.getContext();

      String publisher = context.getPublisherId();
      String itemId = context.getItemId();

      switch (notificationType) {
        case "event_notification":
//          log.info(String.format("Event Notification: Publisher = %s. Item ID = %s", publisher, itemId));

          if (!publisher.equals("") && !itemId.equals("") && !itemId.equals("0")) {
            mostPopularWorker.tell(context, getSelf());
          }

          break;

      }
    } else if (message instanceof OrpItemUpdate) {

      OrpItemUpdate itemUpdate = (OrpItemUpdate) message;
      if (!itemUpdate.isItemRecommendable()) {
        mostPopularMerger.tell(new MostPopularMerger.Remove(itemUpdate.getItemId()), getSelf());
      }

    } else if (message instanceof OrpRequest) {
      Context context = ((OrpRequest) message).getContext();

      String publisher = context.getPublisherId();
      int limit = context.getLimit();

//      log.info(String.format("Received Recommendation Request. Publisher = %s. Limit = %d", publisher, limit));

      if (!publisher.equals("")) {
        mostPopularMerger.tell(new MostPopularMerger.Retrieve(context, limit), getSender());
      }
    }
  }
}
