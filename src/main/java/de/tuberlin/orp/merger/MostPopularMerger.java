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
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import akka.routing.ActorRefRoutee;
import akka.routing.BroadcastRoutingLogic;
import akka.routing.Router;
import de.tuberlin.orp.core.OrpContext;
import de.tuberlin.orp.core.Ranking;
import scala.concurrent.duration.Duration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MostPopularMerger extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private Router router;

  private RankingMerger merger;

  private ActorRef filterActor;

  public MostPopularMerger(ActorRef filterActor) {
    this.filterActor = filterActor;
  }

  public static Props create(ActorRef filterActor) {
    return Props.create(MostPopularMerger.class, new MostPopularMergerCreator(filterActor));
  }

  @Override
  public void preStart() throws Exception {
    router = new Router(new BroadcastRoutingLogic());
    merger = new RankingMerger(50);

    // asks every 2 seconds for the intermediate rankings
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(2, TimeUnit.SECONDS), () -> {
      log.info("Asking for intermediate rankings");
      router.route("getRankings", getSelf());
    }, getContext().dispatcher());

  }

  public static class Register {
    private ActorRef worker;

    public Register() {
    }

    public Register(ActorRef worker) {
      this.worker = worker;
    }

    public ActorRef getWorker() {
      return worker;
    }
  }

  public static class Merge {
    private Map<String, Ranking> rankings;

    public Merge() {
    }

    public Merge(Map<String, Ranking> rankings) {
      this.rankings = rankings;
    }

    public Map<String, Ranking> getRankings() {
      return rankings;
    }
  }

  public static class Retrieve {
    private OrpContext context;
    private int limit;

    public Retrieve() {
    }

    public Retrieve(OrpContext context, int limit) {
      this.context = context;
      this.limit = limit;
    }

    public OrpContext getContext() {
      return context;
    }

    public int getLimit() {
      return limit;
    }
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof Register) {

      ActorRef worker = ((Register) message).getWorker();
      log.info("Registration of actor " + worker.toString());
      router.addRoutee(new ActorRefRoutee(worker));

    } else if (message instanceof Merge) {

      log.info("Received intermediate rankings from " + getSender().toString());
      merger.merge(((Merge) message).getRankings());

    } else if (message instanceof Retrieve) {

      OrpContext context = ((Retrieve) message).getContext();
      int limit = ((Retrieve) message).getLimit();
      Ranking ranking = merger.getRanking(context.getPublisherId(), Integer.MAX_VALUE);

      RecommendationFilter.Filter filter = new RecommendationFilter.Filter(context, ranking, limit, getSender());
      filterActor.tell(filter, getSelf());
    }
  }


  private static class MostPopularMergerCreator implements Creator<MostPopularMerger> {
    private final ActorRef filterActor;

    public MostPopularMergerCreator(ActorRef filterActor) {
      this.filterActor = filterActor;
    }

    @Override
    public MostPopularMerger create() throws Exception {
      return new MostPopularMerger(filterActor);
    }
  }
}
