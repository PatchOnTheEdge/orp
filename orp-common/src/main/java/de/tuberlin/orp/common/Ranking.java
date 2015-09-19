/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Ilya Verbitskiy, Patrick Probst
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.tuberlin.orp.common;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.tuberlin.orp.common.message.OrpItemUpdate;
import io.verbit.ski.core.json.Json;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * A Ranking represents a Mapping from Item to Number of Clicks
 */
public class Ranking implements Serializable {
  private LinkedHashMap<String, Long> ranking;
  private LinkedHashMap<Long, OrpItemUpdate> items;

  public Ranking() {
    ranking = new LinkedHashMap<>();
  }

  public Ranking(Ranking ranking) {
    this.ranking = ranking.getRanking();
  }

  public Ranking(Map<String, Long> ranking) {
    this.ranking = new LinkedHashMap<>(ranking);
  }

  public LinkedHashMap<String, Long> getRanking() {
    return ranking;
  }

  public void merge(Ranking ranking) {
    LinkedHashMap<String, Long> newRanking = ranking.getRanking();
    for (String key : newRanking.keySet()) {
      this.ranking.merge(key, newRanking.get(key), Long::sum);
    }
  }

  public Ranking filter(Set<String> keys) {
    Ranking copy = new Ranking(this);
    if (keys != null) {
      for (String key : keys) {
        copy.getRanking().remove(key);
      }
    }
    return copy;
  }

  public void sort() {
    ranking = Utils.sortMapByEntry(ranking, (o1, o2) -> (int) (o2.getValue() - o1.getValue()));
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
  public String getPublisherName(String id){
    String publisherName = "";
    switch (id){
      case "596": return "Sport1";
      case "694": return "Gulli";
      case "1": return publisherName;
      case "2": return publisherName;
      case "3": return publisherName;
      case "1677": return "Tagesspiegel";
      default: return id + " better change that..";
    }
  }
}
