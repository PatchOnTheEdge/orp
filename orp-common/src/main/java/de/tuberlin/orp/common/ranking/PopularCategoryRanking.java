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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * MostPopularRanking represents a Mapping from OrpArticle to Number of Clicks
 */
public class PopularCategoryRanking implements Ranking<PopularCategoryRanking>, Serializable {
  private LinkedHashMap<String, Long> ranking;

  public PopularCategoryRanking() {
    ranking = new LinkedHashMap<>();
  }

  public PopularCategoryRanking(PopularCategoryRanking mostPopularRanking) {
    this.ranking = mostPopularRanking.getRanking();
  }

  public PopularCategoryRanking(Map<String, Long> ranking) {
    this.ranking = new LinkedHashMap<>(ranking);
  }

  public LinkedHashMap<String, Long> getRanking() {
    return ranking;
  }

  @Override
  public void merge(Ranking<PopularCategoryRanking> ranking) {
    LinkedHashMap<String, Long> newRanking = ranking.getRanking();
    for (String key : newRanking.keySet()) {
      this.ranking.merge(key, newRanking.get(key), Long::sum);
    }
  }

  @Override
  public PopularCategoryRanking filter(Set<String> keys) {
    PopularCategoryRanking copy = new PopularCategoryRanking(this);
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
  public Ranking<PopularCategoryRanking> mix(Ranking ranking, double p, int limit) {
    return null;
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
