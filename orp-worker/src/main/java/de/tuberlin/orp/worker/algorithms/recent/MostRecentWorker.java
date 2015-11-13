package de.tuberlin.orp.worker.algorithms.recent;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.LiFoRingBuffer;
import de.tuberlin.orp.common.message.OrpContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Patch on 21.08.2015.
 */
public class MostRecentWorker extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  //Maps a Publisher to his recent OrpArticle Buffer
  private Map<String, LiFoRingBuffer> bufferMap;
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
    this.bufferMap = new HashMap<String, LiFoRingBuffer>();
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

      LiFoRingBuffer buffer = bufferMap.getOrDefault(context.getPublisherId(), new LiFoRingBuffer(this.bufferSize));

      buffer.add(context);

    } else if (message.equals("getIntermediateRanking")) {

      log.info("Intermediate ranking requested.");
      getSender().tell(bufferMap, getSelf());

    } else {
      unhandled(message);
    }
  }
}
