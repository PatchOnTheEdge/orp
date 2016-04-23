package de.tuberlin.orp.worker.algorithms.mostRecent;

import de.tuberlin.orp.common.ranking.MostRecentRanking;
import de.tuberlin.orp.common.ranking.Ranking;
import de.tuberlin.orp.common.repository.RankingRepository;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.LinkedHashMap;


/**
 * Created by patch on 06.12.2015.
 */
public class RecentArticlesBuffer {

  private int contextWindowSize;
  private int topListSize;

  private HashMap<String, ArrayDeque<String>> recentArticles;

  public RecentArticlesBuffer(int contextWindowSize, int topListSize) {
    this.contextWindowSize = contextWindowSize;
    this.topListSize = topListSize;
    this.recentArticles = new HashMap<>();
  }


  public RankingRepository getRankingRepository() {
    HashMap<String, Ranking> rankings = new HashMap<>();

    for (String publisher : recentArticles.keySet()) {
      ArrayDeque<String> items = recentArticles.get(publisher);
      LinkedHashMap<String, Long> itemRanking = new LinkedHashMap<>();

      for (String item : items) {
        itemRanking.put(item, 10L);
      }

      MostRecentRanking mostRecentRanking = new MostRecentRanking(itemRanking);
      mostRecentRanking.slice(topListSize);
      rankings.put(publisher, mostRecentRanking);
    }

    return new RankingRepository(rankings, new MostRecentRanking());
  }

  public void add(String publisherId, String itemId) {
    ArrayDeque<String> articles = this.recentArticles.getOrDefault(publisherId, new ArrayDeque<>(contextWindowSize));

    if (articles.size() >= contextWindowSize) {
      articles.removeFirst();
    }
    articles.add(itemId);
    this.recentArticles.put(publisherId, articles);
  }

}
