package io.verbit.orp.akka.actors;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import io.verbit.orp.core.Ranking;
import io.verbit.orp.core.Context;
import io.verbit.orp.core.RankingMerger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MostPopularMerger extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private RankingMerger merger;

  private Set<String> removed;
  private Map<String, Long> lastUpdated;
  private Map<String, Set<String>> recommended;


  public static Props create() {
    return Props.create(MostPopularMerger.class, () -> {
      return new MostPopularMerger(50);
    });
  }

  public MostPopularMerger(int windowSize) {
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
    private Context context;
    private int limit;

    public Retrieve() {
    }

    public Retrieve(Context context, int limit) {
      this.context = context;
      this.limit = limit;
    }

    public Context getContext() {
      return context;
    }

    public int getLimit() {
      return limit;
    }
  }


  @Override
  public void preStart() throws Exception {
    super.preStart();
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

      Context context = ((Retrieve) message).getContext();
      int limit = ((Retrieve) message).getLimit();
//      log.info(String.format("Received GetMessage Message. Publisher-ID = %s", publisher));

      Ranking ranking = merger.getRanking(context.getPublisherId(), Integer.MAX_VALUE);

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
