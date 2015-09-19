package de.tuberlin.orp.worker;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.message.OrpContext;

/**
 * Created by Patch on 21.08.2015.
 */
public class MostRecentlyWorker extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private LiFoRingBuffer buffer;

  static class MostRecentlyWorkerCreator implements Creator<MostRecentlyWorker>{
    private int bufferSize;
    public MostRecentlyWorkerCreator(int bufferSize){
      this.bufferSize=bufferSize;
    }
    @Override
    public MostRecentlyWorker create() throws Exception {
      return new MostRecentlyWorker(bufferSize);
    }
  }
  public static Props create(int bufferSize) {
    return Props.create(MostRecentlyWorker.class, new MostRecentlyWorkerCreator(bufferSize));
  }
  public MostRecentlyWorker(int bufferSize) {
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
      //RankingRepository rankingRepository = contextCounter.getRankingRespository();
      //getSender().tell(new WorkerCoordinator.IntermediateRanking(rankingRepository), getSelf());

    } else {
      unhandled(message);
    }
  }
}
