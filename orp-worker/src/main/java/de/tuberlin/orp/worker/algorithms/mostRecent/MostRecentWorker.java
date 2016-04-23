package de.tuberlin.orp.worker.algorithms.mostRecent;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.repository.RankingRepository;
import de.tuberlin.orp.worker.RequestCoordinator;

/**
 * Created by Patch on 21.08.2015.
 */
public class MostRecentWorker extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private int contextWindowSize;
  private int topListSize;

  //Maps a Publisher to his recent OrpArticle Buffer
  private RecentArticlesBuffer recentArticles;


  public MostRecentWorker(int contextWindowSize, int topListSize) {
    this.contextWindowSize = contextWindowSize;
    this.topListSize = topListSize;
    this.recentArticles = new RecentArticlesBuffer(contextWindowSize, topListSize);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    log.info("Most Recent Worker started!");
    log.info(getSelf().toString());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpContext) {

      OrpContext context = (OrpContext) message;
      String publisherId = context.getPublisherId();
      String itemId = context.getItemId();

      log.debug(String.format("Received OrpArticle from Publisher: %s with ID: %s", publisherId, itemId));

      recentArticles.add(publisherId, itemId);

    } else if (message.equals("getIntermediateRanking")) {

      RankingRepository rankingRepository = recentArticles.getRankingRepository();
//      log.debug("Intermediate ranking requested." + rankingRepository.toString());

      getSender().tell(new RequestCoordinator.IntermediateRanking(rankingRepository), getSelf());

    } else {
      unhandled(message);
    }
  }

  static class MostRecentlyWorkerCreator implements Creator<MostRecentWorker>{

    private int contextWindowSize;
    private int topListSize;

    public MostRecentlyWorkerCreator(int contextWindowSize, int topListSize){
      this.contextWindowSize = contextWindowSize;
      this.topListSize = topListSize;
      this.contextWindowSize = contextWindowSize;
    }

    @Override
    public MostRecentWorker create() throws Exception {
      return new MostRecentWorker(contextWindowSize, topListSize);
    }
  }
  public static Props create(int contextWindowSize, int topListSize) {
    return Props.create(MostRecentWorker.class, new MostRecentlyWorkerCreator(contextWindowSize, topListSize));
  }
}
