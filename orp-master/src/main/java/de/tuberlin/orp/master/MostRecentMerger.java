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

  private RankingRepository merger;


  public static Props create() {
    return Props.create(MostRecentMerger.class, new MostRecentMergerCreator());
  }

  @Override
  public void preStart() throws Exception {
    log.info("Most Recent Merger started");

    merger = new RankingRepository(new MostPopularRanking());

    workerRouter = getContext().actorOf(FromConfig.getInstance().props(Props.empty()), "workerRouter");


    // asks every 2 seconds for the intermediate ranking
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(2, TimeUnit.SECONDS), () -> {

      merger.sortRankings();
      log.debug(merger.toString());
      workerRouter.tell(new Broadcast(new MergedRanking(merger)), getSelf());
      merger = new RankingRepository(new MostPopularRanking());

    }, getContext().dispatcher());

  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof MostRecentMerger.WorkerResult) {

      // build cache and send it after timeout

      WorkerResult result = (WorkerResult) message;
      log.debug("Received intermediate ranking from " + getSender().toString());

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

    public WorkerResult(RankingRepository rankingRepository) {
      this.rankings = rankingRepository;
    }

    public RankingRepository getRankingRepository() {
      return rankings;
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

    public MergedRanking(RankingRepository rankingRepository) {
      this.rankingRepository = rankingRepository;
    }

    public RankingRepository getRankingRepository() {
      return rankingRepository;
    }

  }
}
