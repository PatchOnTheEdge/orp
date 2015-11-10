package de.tuberlin.orp.common.repository;

import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.ranking.MostRecentRanking;
import de.tuberlin.orp.common.ranking.Ranking;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by Patch on 20.10.2015.
 */
public class MostRecentRankingRepository extends RankingRepository {
  //Maps a publisher to his item ranking
  private Map<String, Ranking<MostRecentRanking>> rankings;

  public MostRecentRankingRepository() {
    this(new HashMap<>());
  }

  public MostRecentRankingRepository(Map<String, Ranking<MostRecentRanking>> rankings) {
    this.rankings = rankings;
  }
  @Override
  public Optional<Ranking> getRanking(String key) {
    return null;
  }

  @Override
  public Map<String, Ranking<MostPopularRanking>> getRankings() {
    return null;
  }

  @Override
  public void merge(RankingRepository repository) {

  }

  @Override
  public void sortRankings() {

  }
}
