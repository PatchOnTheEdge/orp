package de.tuberlin.orp.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import akka.routing.Broadcast;
import akka.routing.FromConfig;
import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.ranking.RankingFilter;
import de.tuberlin.orp.common.repository.RankingRepository;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Created by Patch on 25.01.2016.
 */
public class FilterMerger extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private ActorRef workerRouter;

  private RankingFilter filter;

  public static Props create() {
    return Props.create(FilterMerger.class, new FilterMergerCreator());
  }

  @Override
  public void preStart() throws Exception {
    log.info("Filter Merger started");

    filter = new RankingFilter();

    workerRouter = getContext().actorOf(FromConfig.getInstance().props(Props.empty()), "workerRouter5");


    // asks every 2 seconds for the intermediate ranking
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(2, TimeUnit.SECONDS), () -> {

      workerRouter.tell(new Broadcast(new MergedFilter(filter)), getSelf());

    }, getContext().dispatcher());

  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof FilterMerger.FilterResult) {

      FilterMerger.FilterResult filterResult = (FilterMerger.FilterResult) message;
      filter.merge(filterResult.getFilter());

    } else if (message.equals("getMergerResult")){
      getSender().tell(this.filter,getSelf());
    }
    else {
      unhandled(message);
    }
  }


  public static class FilterResult implements Serializable {
    private final RankingFilter filter;

    public FilterResult(RankingFilter filter) {
      this.filter = filter;
    }

    public RankingFilter getFilter() {
      return filter;
    }
  }

  private static class MostPopularMergerCreator implements Creator<MostPopularMerger> {
    @Override
    public MostPopularMerger create() throws Exception {
      return new MostPopularMerger();
    }
  }

  public static class MergedFilter implements Serializable {
    private RankingFilter filter;

    public MergedFilter(RankingFilter filter) {
      this.filter = filter;
    }


    public RankingFilter getFilter() {
      return filter;
    }
  }

  private static class FilterMergerCreator implements Creator<FilterMerger> {
    @Override
    public FilterMerger create() throws Exception {
      return new FilterMerger();
    }
  }
}
