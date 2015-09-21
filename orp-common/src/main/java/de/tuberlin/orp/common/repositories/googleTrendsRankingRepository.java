package de.tuberlin.orp.common.repositories;

import de.tuberlin.orp.common.rankings.MostPopularRanking;

import java.util.Map;
import java.util.Optional;

/**
 * Created by Patch on 20.09.2015.
 */
public class googleTrendsRankingRepository implements RankingRepository {
  @Override
  public Optional<MostPopularRanking> getRanking(String key) {
    return null;
  }

  @Override
  public Map<String, MostPopularRanking> getRankings() {
    return null;
  }

  @Override
  public void merge(MostPopularRankingRepository repository) {

  }

  @Override
  public void sortRankings() {

  }
}
