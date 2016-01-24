package de.tuberlin.orp.worker.algorithms.popularCategory;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.repository.RankingRepository;
import de.tuberlin.orp.worker.RequestCoordinator;
import de.tuberlin.orp.worker.algorithms.mostPopular.OrpContextCounter;

/**
 * Created by Patch on 13.01.2016.
 */
public class PopularCategoryWorker extends UntypedActor{
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private CategoryContextCounter contextCounter;

  private long lastRanking = 0;

  static class PopularCategoryActorCreator implements Creator<PopularCategoryWorker> {

    private int contextWindowSize;
    private int topListSize;

    public PopularCategoryActorCreator(int contextWindowSize, int topListSize) {
      this.contextWindowSize = contextWindowSize;
      this.topListSize = topListSize;
    }

    @Override
    public PopularCategoryWorker create() throws Exception {
      return new PopularCategoryWorker(contextWindowSize, topListSize);
    }
  }

  /**
   * @see #PopularCategoryWorker(int, int)
   */
  public static Props create(int contextWindowSize, int topListSize) {
    return Props.create(PopularCategoryWorker.class, new PopularCategoryActorCreator(contextWindowSize, topListSize));
  }

  /**
   * @param contextWindowSize
   *     The window size which defines of how many contexts a window should consist.
   * @param topListSize
   *     The maximum size of the ranking for the publishers.
   */
  public PopularCategoryWorker(int contextWindowSize, int topListSize) {
    this.contextCounter = new CategoryContextCounter(contextWindowSize, topListSize);
  }

  @Override
  public void preStart() throws Exception {
    log.info("MostPopularRanking Worker started.");
    super.preStart();
    log.info(getSelf().toString());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpContext) {
      OrpContext context = (OrpContext) message;

//      log.info(String.format("Received OrpArticle from Publisher: %s with ID: %s",
//          context.getContext(), context.getItemId()));

      contextCounter.add(context);

    } else if (message.equals("getIntermediateRanking")) {

      RankingRepository rankingRepository = contextCounter.getRankingRepository();
      //log.info("Intermediate ranking requested.");
      getSender().tell(new RequestCoordinator.IntermediateRanking(rankingRepository), getSelf());

    } else {
      unhandled(message);
    }
  }
}
