package io.verbit.orp.akka.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import io.verbit.orp.core.Context;
import io.verbit.orp.core.Ranking;
import io.verbit.orp.core.ContextCounter;
import scala.concurrent.duration.Duration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An actor that can receive items and store them per publisher in a private Hashmap. The top n entries will be send to
 * the MostPopularMerger in a defined interval.
 */
public class MostPopularActor extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private ContextCounter contextCounter;
  private ActorRef mostPopularMerger;

  private long lastRanking = 0;
  private int receivedImpressions = 0;


  /**
   * @see #MostPopularActor(ActorRef, int, int)
   */
  public static Props create(ActorRef mostPopularMerger, int contextWindowSize, int topListSize) {
    return Props.create(MostPopularActor.class, () -> {
      return new MostPopularActor(mostPopularMerger, contextWindowSize, topListSize);
    });
  }

  /**
   * @param mostPopularMerger
   *     A reference to the actor which will merge the intermediate rankings to a total ranking.
   * @param contextWindowSize
   *     The window size which defines of how many contexts a window should consist.
   * @param topListSize
   *     The maximum size of the rankings for the publishers.
   */
  public MostPopularActor(ActorRef mostPopularMerger, int contextWindowSize, int topListSize) {
    this.mostPopularMerger = mostPopularMerger;
    this.contextCounter = new ContextCounter(contextWindowSize, topListSize);
  }


  public void preStart() throws Exception {
    super.preStart();

    getContext().system().scheduler().schedule(
        Duration.Zero(),
        Duration.create(2, TimeUnit.SECONDS), () -> {

          if (lastRanking != 0) {
            double throughput = 1000.0 * receivedImpressions / (System.currentTimeMillis() - lastRanking);
            log.info(String.format("Throughput: %.2f impressions/sec", throughput));
          }

          lastRanking = System.currentTimeMillis();
          receivedImpressions = 0;

          Map<String, Ranking> rankings = contextCounter.getRankings();
//          log.info(rankings.toString());

          mostPopularMerger.tell(new MostPopularMerger.Merge(rankings), getSelf());

        }, getContext().dispatcher());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof Context) {
      Context context = (Context) message;

//      log.info(String.format("Received Item from Publisher: %s with ID: %s",
//          context.getContext(), context.getItemId()));

      ++receivedImpressions;
      contextCounter.add(context);

    }
  }
}
