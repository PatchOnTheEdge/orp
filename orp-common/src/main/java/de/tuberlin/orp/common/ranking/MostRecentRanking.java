package de.tuberlin.orp.common.ranking;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.tuberlin.orp.common.Utils;
import io.verbit.ski.core.json.Json;

import java.io.Serializable;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Created by Patch on 20.10.2015.
 */
public class MostRecentRanking implements Ranking<MostRecentRanking>, Serializable{
  private LinkedHashMap<String, Date> ranking;

  public MostRecentRanking() {
    this.ranking = new LinkedHashMap<>();
  }

  public MostRecentRanking(LinkedHashMap<String, Date> ranking) {
    this.ranking = ranking;
  }

  public MostRecentRanking(MostRecentRanking mostRecentRanking) {
    this.ranking = new LinkedHashMap<>(ranking);
  }

  public LinkedHashMap<String, Date> getRanking() {
    return this.ranking;
  }

  @Override
  public void merge(Ranking<MostRecentRanking> ranking) {
    LinkedHashMap<String, Date> newRanking = ranking.getRanking();
    for (String key : newRanking.keySet()) {
      this.ranking.merge(key, newRanking.get(key), this::newestDate);
    }
  }

  private Date newestDate(Date date1, Date date2) {
    if (date1.after(date2)){
      return date1;
    }
    return date2;
  }


  @Override
  public Ranking<MostRecentRanking> filter(Set<String> keys) {
    MostRecentRanking copy = new MostRecentRanking(this);
    if (keys != null){
      for (String key : keys) {
        copy.getRanking().remove(key);
      }
    }
    return copy;
  }

  @Override
  public void sort() {
    ranking = Utils.sortMapByEntry(this.ranking, (d1,d2) -> d1.getValue().compareTo(d2.getValue()));
  }

  @Override
  public void slice(int limit) {
    this.ranking = Utils.sliceMap(this.ranking, limit);
  }

  @Override
  public ArrayNode toJson() {
    ObjectNode jsonNodes = Json.newObject();
    ArrayNode rankings = jsonNodes.putArray("ranking");
    for (String key : this.ranking.keySet()) {
      ObjectNode newRank = Json.newObject();
      newRank.put("key",key);
      newRank.put("rank",this.ranking.get(key).toString());
      rankings.add(newRank);
    }
    return rankings;
  }
}
