package de.tuberlin.orp.worker.algorithms.mostRecent;

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

}
