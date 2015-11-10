package de.tuberlin.orp.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import akka.routing.Broadcast;
import akka.routing.FromConfig;
import de.tuberlin.orp.common.ranking.RankingFilter;
import de.tuberlin.orp.common.repository.MostRecentRankingRepository;
import de.tuberlin.orp.common.repository.RankingRepository;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Created by Patch on 20.10.2015.
 */
public class MostRecentMerger extends UntypedActor{
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private ActorRef workerRouter;

  private RankingFilter filter;
  private RankingRepository merger;


  public static Props create() {
    return Props.create(MostRecentMerger.class, new MostRecentMergerCreator());
  }

  @Override
  public void preStart() throws Exception {
    log.info("Most Recent Merger started");

    merger = new MostRecentRankingRepository();
    filter = new RankingFilter();

    workerRouter = getContext().actorOf(FromConfig.getInstance().props(Props.empty()), "workerRouter");


    // asks every 2 seconds for the intermediate ranking
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(2, TimeUnit.SECONDS), () -> {

      merger.sortRankings();
      log.debug(merger.toString());
      workerRouter.tell(new Broadcast(new MergedRanking(merger, filter)), getSelf());
      merger = new MostRecentRankingRepository();

    }, getContext().dispatcher());

  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof WorkerResult) {

      // build cache and send it after timeout

      WorkerResult result = (WorkerResult) message;
      //log.info("Received intermediate ranking from " + getSender().toString());

      filter.merge(result.getFilter());
      merger.merge(result.getRankingRepository());

    }
    else if (message.equals("getMergerResult")){
      getSender().tell(this.merger.getRankings(),getSelf());
    }
    else {
      unhandled(message);
    }
  }


  public static class WorkerResult implements Serializable {
    private final RankingRepository rankings;
    private final RankingFilter filter;

    public WorkerResult(RankingRepository rankingRepository, RankingFilter filter) {
      this.rankings = rankingRepository;
      this.filter = filter;
    }

    public RankingRepository getRankingRepository() {
      return rankings;
    }

    public RankingFilter getFilter() {
      return filter;
    }
  }

  private static class MostRecentMergerCreator implements Creator<MostRecentMerger> {
    @Override
    public MostRecentMerger create() throws Exception {
      return new MostRecentMerger();
    }
  }

  public static class MergedRanking implements Serializable {
    private RankingRepository rankingRepository;
    private RankingFilter filter;

    public MergedRanking(RankingRepository rankingRepository, RankingFilter filter) {
      this.rankingRepository = rankingRepository;
      this.filter = filter;
    }

    public RankingRepository getRankingRepository() {
      return rankingRepository;
    }

    public RankingFilter getFilter() {
      return filter;
    }
  }
}
