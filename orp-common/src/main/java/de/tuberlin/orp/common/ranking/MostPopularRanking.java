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

import java.util.*;

/**
 * MostPopularRanking represents a Mapping from OrpArticle to Number of Clicks
 */
public class MostPopularRanking extends Ranking<MostPopularRanking> {

  public MostPopularRanking() {
    ranking = new LinkedHashMap<>();
  }

  public MostPopularRanking(MostPopularRanking mostPopularRanking) {
    this.ranking = mostPopularRanking.getRanking();
  }

  public MostPopularRanking(Map<String, Long> ranking) {
    this.ranking = new LinkedHashMap<>(ranking);
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
  public MostPopularRanking mix(Ranking ranking, double p, int limit) {
    LinkedHashMap<String, Long> result = new LinkedHashMap<>();
    Iterator<Map.Entry<String, Long>> iterator1 = ranking.getRanking().entrySet().iterator();
    Iterator<Map.Entry<String, Long>> iterator2 = this.ranking.entrySet().iterator();

    while (result.size() < limit){
      double r = Math.random();
      if (r <= p && iterator1.hasNext()){
        Map.Entry<String, Long> entry = iterator1.next();
        result.put(entry.getKey(), entry.getValue());
      } else if (iterator2.hasNext()){
        Map.Entry<String, Long> entry = iterator2.next();
        result.put(entry.getKey(), entry.getValue());
      }
    }
    return new MostPopularRanking(result);
  }
}
