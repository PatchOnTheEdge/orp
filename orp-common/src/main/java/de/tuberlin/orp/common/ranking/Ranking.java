package de.tuberlin.orp.common.ranking;

import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Created by Patch on 20.10.2015.
 */
public interface Ranking<T> extends Serializable {
  LinkedHashMap<String, Long> getRanking();

  void merge(Ranking<T> ranking);

  Ranking<T> filter(Set<String> keys);

  void sort();

  void slice(int limit);

  Ranking<T> mix(Ranking ranking, double p, int limit);

  String toString();

  ArrayNode toJson();
}
