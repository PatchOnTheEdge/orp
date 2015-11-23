package de.tuberlin.orp.master;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.Broadcast;
import de.tuberlin.orp.common.ranking.Ranking;
import de.tuberlin.orp.common.repository.RankingRepository;
import scala.concurrent.duration.Duration;

import java.util.ArrayDeque;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by patch on 23.11.2015.
 */
public class RankingTimeline extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private HashMap<Date, RankingRepository> rankings;
  private ArrayDeque<Date> timestamps;

  @Override
  public void preStart() throws Exception {


    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(10, TimeUnit.MINUTES), () -> {
      //ask Mergers for ther ranking repos and store them
    }, getContext().dispatcher());

    super.preStart();
  }

  @Override
  public void onReceive(Object o) throws Exception {

  }
}
