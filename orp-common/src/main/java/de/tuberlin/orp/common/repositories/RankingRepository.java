package de.tuberlin.orp.common.repositories;

import de.tuberlin.orp.common.rankings.MostPopularRanking;

import java.util.Map;
import java.util.Optional;

/**
 * Created by Patch on 20.09.2015.
 */
public interface RankingRepository {
  Optional<MostPopularRanking> getRanking(String key);

  Map<String, MostPopularRanking> getRankings();

  void merge(MostPopularRankingRepository repository);

  void sortRankings();
}
