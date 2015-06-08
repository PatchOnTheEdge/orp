/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Ilya Verbitskiy
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

package io.verbit.orp.core;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ContextCounter {
  private int contextWindowSize;
  private ArrayDeque<Context> contextWindow;
  private int topListSize;

  public ContextCounter(int contextWindowSize, int topListSize) {
    this.contextWindowSize = contextWindowSize;
    this.contextWindow = new ArrayDeque<>(contextWindowSize);
    this.topListSize = topListSize;
  }

  public boolean isFull() {
    return contextWindow.size() == contextWindowSize;
  }

  public void add(Context context) {
    if (contextWindow.size() >= contextWindowSize) {
      contextWindow.removeFirst();
    }

    contextWindow.add(context);
  }

  public Map<String, Ranking> getRankings() {
    Map<String, Map<String, Long>> countMap = calculateRankings();


    Map<String, Ranking> rankings = new HashMap<>(countMap.size());
    for (Map.Entry<String, Map<String, Long>> entry : countMap.entrySet()) {
      String key = entry.getKey();
      Ranking value = new Ranking(entry.getValue());
      value.sort();
      value.slice(topListSize);
      rankings.put(key, value);
    }

    return rankings;
  }

  private Map<String, Map<String, Long>> calculateRankings() {
    return contextWindow.stream()
        .collect(
            Collectors.groupingBy(
                Context::getPublisherId,
                Collectors.groupingBy(Context::getItemId, Collectors.counting())
            ));
  }
}
