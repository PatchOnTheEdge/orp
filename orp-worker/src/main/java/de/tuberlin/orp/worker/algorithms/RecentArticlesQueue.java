package de.tuberlin.orp.worker.algorithms;

import de.tuberlin.orp.common.message.OrpArticle;
import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.ranking.MostRecentRanking;
import de.tuberlin.orp.common.ranking.Ranking;
import de.tuberlin.orp.common.repository.RankingRepository;

import java.util.ArrayDeque;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by patch on 06.12.2015.
 */
public class RecentArticlesQueue {

  private int contextWindowSize;
  private int topListSize;

  private ArrayDeque<OrpArticle> recentArticles;

  public RecentArticlesQueue(int contextWindowSize, int topListSize) {
    this.contextWindowSize = contextWindowSize;
    this.topListSize = topListSize;
    this.recentArticles = new ArrayDeque<>(contextWindowSize);
  }

  public void add(OrpArticle article) {
    if (recentArticles.size() >= contextWindowSize) {
      recentArticles.removeFirst();
    }

    recentArticles.add(article);
  }

  public RankingRepository getRankingRespository() {
    Map<String, Map<String, Date>> countMap = calculateRankings();

    Map<String, Ranking> rankings = new HashMap<>(countMap.size());
    for (Map.Entry<String, Map<String, Long>> entry : countMap.entrySet()) {
      String key = entry.getKey();
      MostRecentRanking value = new MostRecentRanking(entry.getValue());
      value.sort();
      value.slice(topListSize);
      rankings.put(key, value);
    }

    return new RankingRepository(rankings, new MostPopularRanking());
  }

  private Map<String, OrpArticle> calculateRankings() {
    return recentArticles.stream()
        .collect(
            Collectors.groupingBy(
                OrpArticle::getPublisherId,
                Collectors.)
            );
  }
}