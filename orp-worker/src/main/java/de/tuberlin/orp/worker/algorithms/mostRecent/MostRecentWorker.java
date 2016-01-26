package de.tuberlin.orp.worker.algorithms.mostRecent;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.ranking.MostRecentRanking;
import de.tuberlin.orp.common.ranking.Ranking;
import de.tuberlin.orp.common.repository.RankingRepository;
import de.tuberlin.orp.master.MostRecentMerger;
import de.tuberlin.orp.worker.RequestCoordinator;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Patch on 21.08.2015.
 */
public class MostRecentWorker extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private int contextWindowSize;

  //Maps a Publisher to his recent OrpArticle Buffer
  private RankingRepository rankingRepository;

  private RecentArticlesQueue recentArticles;

  static class MostRecentlyWorkerCreator implements Creator<MostRecentWorker>{

    private int contextWindowSize;
    private int topListSize;

    public MostRecentlyWorkerCreator(int contextWindowSize, int topListSize){
      this.contextWindowSize = contextWindowSize;
      this.topListSize = topListSize;
    }

    @Override
    public MostRecentWorker create() throws Exception {
      return new MostRecentWorker(contextWindowSize, topListSize);
    }
  }
  public static Props create(int contextWindowSize, int topListSize) {
    return Props.create(MostRecentWorker.class, new MostRecentlyWorkerCreator(contextWindowSize, topListSize));
  }
  public MostRecentWorker(int contextWindowSize, int topListSize) {
    this.contextWindowSize = contextWindowSize;
    this.rankingRepository = new RankingRepository(new MostPopularRanking());
    this.recentArticles = new RecentArticlesQueue(contextWindowSize, topListSize);
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

      Map<String, Ranking> rankings = rankingRepository.getRankings();
      MostRecentRanking ranking = (MostRecentRanking) rankings.getOrDefault(publisherId, new MostRecentRanking());

      LinkedHashMap<String, Long> map = new LinkedHashMap<String, Long>();
      map.put(itemId, Instant.now().getEpochSecond());
      MostRecentRanking newRanking = new MostRecentRanking(map);
      ranking.mergeAndSlice(newRanking, this.contextWindowSize);

      rankings.put(publisherId, ranking);

      RankingRepository newRepo = new RankingRepository(rankings, new MostRecentRanking());

      this.rankingRepository.merge(newRepo);

    } else if (message.equals("getIntermediateRanking")) {

//      log.info("Intermediate ranking requested." + rankingRepository.toString());
      getSender().tell(new RequestCoordinator.IntermediateRanking(rankingRepository), getSelf());

    } else if (message instanceof MostRecentMerger.MergedRanking) {

      MostRecentMerger.MergedRanking mergedRanking = (MostRecentMerger.MergedRanking) message;
      this.rankingRepository = mergedRanking.getRankingRepository();

    } else {
      unhandled(message);
    }
  }
}
