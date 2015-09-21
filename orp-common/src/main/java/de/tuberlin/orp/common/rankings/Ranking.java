package de.tuberlin.orp.common.rankings;

import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Created by Patch on 20.09.2015.
 */
public interface Ranking {

  LinkedHashMap<String, Long> getRanking();

  void merge(MostPopularRanking mostPopularRanking);

  Ranking filter(Set<String> keys);

  void sort();

  void slice(int limit);

  ArrayNode toJson();
}
