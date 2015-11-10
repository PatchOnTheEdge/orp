package de.tuberlin.orp.common.ranking;

import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Created by Patch on 20.10.2015.
 */
public class MostRecentRanking implements Ranking<MostRecentRanking>{
  private LinkedHashMap<String, Long> ranking;

  public MostRecentRanking() {
    this.ranking = new LinkedHashMap<>();
  }

  public MostRecentRanking(LinkedHashMap<String, Long> ranking) {
    this.ranking = ranking;
  }

  @Override
  public LinkedHashMap<String, Long> getRanking() {
    return this.ranking;
  }

  @Override
  public void merge(Ranking<MostRecentRanking> ranking) {
    LinkedHashMap<String, Long> newRanking = ranking.getRanking();
    for (String key : newRanking.keySet()) {
      this.ranking.merge(key, newRanking.get(key), Long::sum);
    }
  }

  @Override
  public Ranking<MostRecentRanking> filter(Set<String> keys) {
    return null;
  }

  @Override
  public void sort() {

  }

  @Override
  public void slice(int limit) {

  }

  @Override
  public ArrayNode toJson() {
    return null;
  }
}
