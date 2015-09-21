package de.tuberlin.orp.worker.algorithms.recent;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.LiFoRingBuffer;
import de.tuberlin.orp.common.messages.OrpContext;

/**
 * Created by Patch on 21.08.2015.
 */
public class MostRecentWorker extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private LiFoRingBuffer buffer;

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
    this.buffer = new LiFoRingBuffer(bufferSize);
  }
  @Override
  public void preStart() throws Exception {
    super.preStart();
    log.info(getSelf().toString());
  }
  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpContext) {
      OrpContext context = (OrpContext) message;

      buffer.add(context);

    } else if (message.equals("getIntermediateRanking")) {

      log.info("Intermediate rankings requested.");
      //MostPopularRankingRepository rankingRepository = contextCounter.getRankingRespository();
      //getSender().tell(new RequestCoordinator.IntermediateRanking(rankingRepository), getSelf());

    } else {
      unhandled(message);
    }
  }
}
