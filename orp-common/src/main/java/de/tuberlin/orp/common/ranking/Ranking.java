package de.tuberlin.orp.common.ranking;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.tuberlin.orp.common.Utils;
import io.verbit.ski.core.json.Json;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Patch on 20.10.2015.
 */
public abstract class Ranking<T> implements Serializable {
  LinkedHashMap<String, Long> ranking;

  public LinkedHashMap<String, Long> getRanking() {
    return this.ranking;
  }

  public void sort() {
    this.ranking = Utils.sortMapByEntry(this.ranking, (o1, o2) -> (int) (o2.getValue() - o1.getValue()));
  }

  public void slice(int limit) {
    ranking = Utils.sliceMap(ranking, limit);
  }

  @Override
  public String toString() {
    return ranking.toString();
  }

  public ArrayNode toJson(){
    ObjectNode jsonNodes = Json.newObject();
    ArrayNode rankings = jsonNodes.putArray("ranking");
    for (String key : this.ranking.keySet()) {
      ObjectNode newRank = Json.newObject();
      newRank.put("key",key);
      newRank.put("rank",this.ranking.get(key));
      rankings.add(newRank);
    }
    return rankings;
  }

  public abstract Ranking<T> mix(Ranking ranking, double p, int limit);

  public abstract void merge(Ranking<T> ranking);

  public abstract Ranking<T> filter(Set<String> keys) ;

}
