/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Ilya Verbitskiy, Patrick Probst
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.tuberlin.orp.merger;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.Broadcast;
import akka.routing.FromConfig;
import de.tuberlin.orp.core.OrpContext;
import de.tuberlin.orp.core.Ranking;
import de.tuberlin.orp.worker.MostPopularWorker;
import scala.concurrent.duration.Duration;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MostPopularMergerOld extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private ActorRef worker;
  private RankingMerger merger;

  private Set<String> removed;
  private Map<String, Long> lastUpdated;
  private Map<String, Set<String>> recommended;


  public static Props create() {
    return Props.create(MostPopularMergerOld.class, () -> {
      return new MostPopularMergerOld(50);
    });
  }

  public MostPopularMergerOld(int windowSize) {
    merger = new RankingMerger(windowSize);
    removed = new HashSet<>();
    lastUpdated = new HashMap<>();
    recommended = new HashMap<>();
  }


  public static class Merge {
    private Map<String, Ranking> rankings;

    public Merge() {
    }

    public Merge(Map<String, Ranking> rankings) {
      this.rankings = rankings;
    }

    public Map<String, Ranking> getRankings() {
      return rankings;
    }
  }

  public static class Remove {
    private String itemId;

    public Remove() {
    }

    public Remove(String itemId) {
      this.itemId = itemId;
    }

    public String getItemId() {
      return itemId;
    }
  }

  public static class Clicked {
    private String userId;
    private String itemId;

    public Clicked() {
    }

    public Clicked(String userId, String itemId) {
      this.userId = userId;
      this.itemId = itemId;
    }

    public String getUserId() {
      return userId;
    }

    public String getItemId() {
      return itemId;
    }
  }

  public static class Retrieve {
    private OrpContext context;
    private int limit;

    public Retrieve() {
    }

    public Retrieve(OrpContext context, int limit) {
      this.context = context;
      this.limit = limit;
    }

    public OrpContext getContext() {
      return context;
    }

    public int getLimit() {
      return limit;
    }
  }


  @Override
  public void preStart() throws Exception {
    super.preStart();

    worker = getContext().actorOf(FromConfig.getInstance().props(MostPopularWorker.create(500, 50)), "worker");

    getContext().system().scheduler().schedule(
        Duration.Zero(),
        Duration.create(2, TimeUnit.SECONDS), () -> {

          this.worker.tell(new Broadcast("getRankings"), getSelf());

        }, getContext().dispatcher());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof Merge) {

      merger.merge(((Merge) message).getRankings());

    } else if (message instanceof Remove) {

      removed.add(((Remove) message).getItemId());

    } else if (message instanceof Clicked) {

      Clicked clicked = (Clicked) message;
      recommended.putIfAbsent(clicked.getUserId(), new HashSet<>());
      Set<String> itemsRecommended = recommended.get(clicked.getUserId());
      itemsRecommended.add(clicked.getItemId());

      lastUpdated.put(clicked.getUserId(), System.currentTimeMillis());

    } else if (message instanceof Retrieve) {

      cleanRecommended();

      OrpContext context = ((Retrieve) message).getContext();
      int limit = ((Retrieve) message).getLimit();
//      log.info(String.format("Received GetMessage Message. Publisher-ID = %s", publisher));

      Ranking ranking = merger.getRanking(context.getPublisherId(), Integer.MAX_VALUE);

//      log.debug("ranking = " + Objects.toString(ranking));

      if (ranking == null) {
        getSender().tell(new Ranking(), getSelf());
      } else {
        ranking.filter(removed);
        ranking.filter(recommended.getOrDefault(context.getUserId(), Collections.emptySet()));
        ranking.filter(new HashSet<>(Arrays.asList(context.getItemId())));
        ranking.slice(limit);
//        log.info("GetMessage Map");
//        log.info(ranking.toString());

        getSender().tell(ranking, getSelf());
      }

      removed.clear();
    }
  }

  /**
   * For a fixed time period recommended items for users are remembered in a map. Such a map will be cleaned so that its
   * size won't explode.
   */
  private void cleanRecommended() {
    long now = System.currentTimeMillis();
    Set<String> toRemove = new HashSet<>();
    for (String key : lastUpdated.keySet()) {
      long userLastUpdated = lastUpdated.get(key);
      if (now - userLastUpdated > 1000 * 60 * 30) {
        toRemove.add(key);
      }
    }
    for (String key : toRemove) {
      lastUpdated.remove(key);
      recommended.remove(key);
    }
  }
}
