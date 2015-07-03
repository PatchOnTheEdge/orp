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

package de.tuberlin.orp.master;

import de.tuberlin.orp.common.Ranking;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

public class RankingMerger {
  private Map<String, Ranking> mergedRankings = new HashMap<>();


  public void merge(Map<String, Ranking> rankings) {
    merge(mergedRankings, rankings);
  }

  private void merge(Map<String, Ranking> mergedRankings, Map<String, Ranking> rankings) {

    for (String publisher : rankings.keySet()) {
      mergedRankings.putIfAbsent(publisher, new Ranking());
      Ranking ranking = mergedRankings.get(publisher);
      ranking.merge(rankings.get(publisher));
    }

  }

  public Map<String, Ranking> merge() {
    Map<String, Ranking> result = mergedRankings;
    mergedRankings = new HashMap<>();
    return result;
  }

  public Ranking getRanking(Map<String, Ranking> mergedRankings, String key) {
    Ranking ranking = mergedRankings.get(key);

    if (ranking == null) {
      return new Ranking(new HashMap<>());
    }

    ranking.sort();
    return ranking;
  }
}
