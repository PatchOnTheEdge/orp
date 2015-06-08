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

package de.tuberlin.orp.core;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

public class RankingMerger {
  private int windowSize;
  private ArrayDeque<Map<String, Ranking>> mergedRankingsWindow;


  public RankingMerger(int windowSize) {
    this.windowSize = windowSize;
    this.mergedRankingsWindow = new ArrayDeque<>(windowSize);
  }


  public void merge(Map<String, Ranking> rankings) {
    if (mergedRankingsWindow.size() >= windowSize) {
      mergedRankingsWindow.removeFirst();
    }

    mergedRankingsWindow.add(rankings);
  }

  private void merge(Map<String, Ranking> mergedRankings, Map<String, Ranking> rankings) {

    for (String publisher : rankings.keySet()) {
      mergedRankings.putIfAbsent(publisher, new Ranking());
      Ranking ranking = mergedRankings.get(publisher);
      ranking.merge(rankings.get(publisher));
    }

  }

  public Ranking getRanking(String key, int topListSize) {
    Map<String, Ranking> mergedRankings = new HashMap<>();

    for (Map<String, Ranking> rankings : mergedRankingsWindow) {
      merge(mergedRankings, rankings);
    }

    Ranking ranking = mergedRankings.get(key);

    if (ranking == null) {
      return null;
    }

    ranking.sort();
    ranking.slice(topListSize);
    return ranking;
  }
}
