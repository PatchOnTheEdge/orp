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

package de.tuberlin.orp.common.ranking;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.tuberlin.orp.common.Utils;
import io.verbit.ski.core.json.Json;

import java.io.Serializable;
import java.util.*;

/**
 * MostPopularRanking represents a Mapping from OrpArticle to Number of Clicks
 */
public class MostPopularRanking implements Ranking<MostPopularRanking>, Serializable {
  private LinkedHashMap<String, Long> ranking;

  public MostPopularRanking() {
    ranking = new LinkedHashMap<>();
  }

  public MostPopularRanking(MostPopularRanking mostPopularRanking) {
    this.ranking = mostPopularRanking.getRanking();
  }

  public MostPopularRanking(Map<String, Long> ranking) {
    this.ranking = new LinkedHashMap<>(ranking);
  }

  public LinkedHashMap<String, Long> getRanking() {
    return ranking;
  }

  @Override
  public void merge(Ranking<MostPopularRanking> ranking) {
    LinkedHashMap<String, Long> newRanking = ranking.getRanking();
    for (String key : newRanking.keySet()) {
      this.ranking.merge(key, newRanking.get(key), Long::sum);
    }
  }

  @Override
  public MostPopularRanking filter(Set<String> keys) {
    MostPopularRanking copy = new MostPopularRanking(this);
    if (keys != null) {
      for (String key : keys) {
        copy.getRanking().remove(key);
      }
    }
    return copy;
  }

  @Override
  public void sort() {
    this.ranking = Utils.sortMapByEntry(this.ranking, (o1, o2) -> (int) (o2.getValue() - o1.getValue()));
  }

  @Override
  public void slice(int limit) {
    ranking = Utils.sliceMap(ranking, limit);
  }

  @Override
  public Ranking<MostPopularRanking> mix(Ranking ranking, double p, int limit) {
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
    return new MostPopularRanking(result);
  }


  @Override
  public String toString() {
    return ranking.toString();
  }

  @Override
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
}
