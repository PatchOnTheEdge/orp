package de.tuberlin.orp.common.rankings;

import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Created by Patch on 20.09.2015.
 */
public class googleTrendsRanking implements Ranking {
  @Override
  public LinkedHashMap<String, Long> getRanking() {
    return null;
  }

  @Override
  public void merge(MostPopularRanking mostPopularRanking) {

  }

  @Override
  public Ranking filter(Set<String> keys) {
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
