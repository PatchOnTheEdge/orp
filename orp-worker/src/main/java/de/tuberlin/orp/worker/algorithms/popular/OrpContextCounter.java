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

package de.tuberlin.orp.worker.algorithms.popular;

import de.tuberlin.orp.common.rankings.MostPopularRanking;
import de.tuberlin.orp.common.repositories.MostPopularRankingRepository;
import de.tuberlin.orp.common.messages.OrpContext;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class OrpContextCounter {
  private int contextWindowSize;
  private ArrayDeque<OrpContext> contextWindow;
  private int topListSize;

  public OrpContextCounter(int contextWindowSize, int topListSize) {
    this.contextWindowSize = contextWindowSize;
    this.contextWindow = new ArrayDeque<>(contextWindowSize);
    this.topListSize = topListSize;
  }

  public boolean isFull() {
    return contextWindow.size() == contextWindowSize;
  }

  public void add(OrpContext context) {
    if (contextWindow.size() >= contextWindowSize) {
      contextWindow.removeFirst();
    }

    contextWindow.add(context);
  }

  public MostPopularRankingRepository getRankingRespository() {
    Map<String, Map<String, Long>> countMap = calculateRankings();

    Map<String, MostPopularRanking> rankings = new HashMap<>(countMap.size());
    for (Map.Entry<String, Map<String, Long>> entry : countMap.entrySet()) {
      String key = entry.getKey();
      MostPopularRanking value = new MostPopularRanking(entry.getValue());
      value.sort();
      value.slice(topListSize);
      rankings.put(key, value);
    }

    return new MostPopularRankingRepository(rankings);
  }

  private Map<String, Map<String, Long>> calculateRankings() {
    return contextWindow.stream()
        .collect(
            Collectors.groupingBy(
                OrpContext::getPublisherId,
                Collectors.groupingBy(OrpContext::getItemId, Collectors.counting())
            ));
  }
}
