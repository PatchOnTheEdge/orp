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

import io.verbit.orp.akka.actors.Utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Ranking {
  private LinkedHashMap<String, Long> ranking;


  public Ranking() {
    ranking = new LinkedHashMap<>();
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

  public void filter(Set<String> keys) {
    for (String key : keys) {
      ranking.remove(key);
    }
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
}
