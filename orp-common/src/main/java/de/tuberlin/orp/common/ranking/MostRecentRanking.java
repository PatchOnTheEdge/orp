package de.tuberlin.orp.common.ranking;

import java.util.*;

/**
 * Created by Patch on 20.10.2015.
 */
public class MostRecentRanking extends Ranking<MostRecentRanking>{

  public MostRecentRanking() {
    this.ranking = new LinkedHashMap<>();
  }

  public MostRecentRanking(LinkedHashMap<String, Long> ranking) {
    this.ranking = new LinkedHashMap<>(ranking);
  }

  public MostRecentRanking(MostRecentRanking mostRecentRanking) {
    this.ranking = mostRecentRanking.getRanking();
  }

  @Override
  public void merge(Ranking<MostRecentRanking> ranking) {
    LinkedHashMap<String, Long> newRanking = ranking.getRanking();
    for (String key : newRanking.keySet()) {
      this.ranking.merge(key, newRanking.get(key), this::newestDate);
    }
  }
  public void mergeAndSlice(Ranking<MostRecentRanking> ranking, int limit) {
    ranking.slice(limit);
    LinkedHashMap<String, Long> newRanking = ranking.getRanking();
    for (String key : newRanking.keySet()) {
      this.ranking.merge(key, newRanking.get(key), this::newestDate);
    }
  }

  private Long newestDate(Long date1, Long date2) {
    if (date1 >= date2){
      return date1;
    }
    return date2;
  }

  @Override
  public MostRecentRanking filter(Set<String> keys) {
    MostRecentRanking copy = new MostRecentRanking(this);
    if (keys != null){
      for (String key : keys) {
        copy.getRanking().remove(key);
      }
    }
    return copy;
  }

  @Override
  public MostRecentRanking mix(Ranking ranking, double p, int limit) {
    LinkedHashMap<String, Long> result = new LinkedHashMap<>();
    Iterator<Map.Entry<String, Long>> iterator1 = ranking.getRanking().entrySet().iterator();
    Iterator<Map.Entry<String, Long>> iterator2 = this.ranking.entrySet().iterator();

    while (result.size() < limit){
      double r = Math.random();
      if (r <= p){
        Map.Entry<String, Long> entry = iterator1.next();
        result.put(entry.getKey(), entry.getValue());
      } else {
        Map.Entry<String, Long> entry = iterator2.next();
        result.put(entry.getKey(), entry.getValue());
      }
    }
    return new MostRecentRanking(result);
  }
}
