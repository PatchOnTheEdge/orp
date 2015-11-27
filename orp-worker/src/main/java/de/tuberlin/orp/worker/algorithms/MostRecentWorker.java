package de.tuberlin.orp.worker.algorithms;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.LiFoRingBuffer;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.ranking.MostRecentRanking;
import de.tuberlin.orp.common.ranking.Ranking;
import de.tuberlin.orp.common.repository.RankingRepository;
import de.tuberlin.orp.master.MostRecentMerger;
import de.tuberlin.orp.worker.RequestCoordinator;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Patch on 21.08.2015.
 */
public class MostRecentWorker extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  //Maps a Publisher to his recent OrpArticle Buffer
  private RankingRepository rankingRepository;
  private int bufferSize;

  static class MostRecentlyWorkerCreator implements Creator<MostRecentWorker>{
    private int bufferSize;
    public MostRecentlyWorkerCreator(int bufferSize){
      this.bufferSize=bufferSize;
    }
    @Override
    public MostRecentWorker create() throws Exception {
      return new MostRecentWorker(bufferSize);
    }
  }
  public static Props create(int bufferSize) {
    return Props.create(MostRecentWorker.class, new MostRecentlyWorkerCreator(bufferSize));
  }
  public MostRecentWorker(int bufferSize) {
    this.rankingRepository = new RankingRepository(new MostRecentRanking());
    this.bufferSize = bufferSize;
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

      log.info(String.format("Received OrpArticle from Publisher: %s with ID: %s", publisherId, itemId));
      Map<String, Ranking> rankings = rankingRepository.getRankings();
      MostRecentRanking ranking = (MostRecentRanking) rankings.getOrDefault(publisherId, new MostRecentRanking());

      LinkedHashMap<String, Date> map = new LinkedHashMap<String, Date>();
      map.put(itemId, new Date());
      MostRecentRanking newRanking = new MostRecentRanking(map);
      ranking.merge(newRanking);

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
