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

package de.tuberlin.orp.common.repositories;

import de.tuberlin.orp.common.rankings.MostPopularRanking;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A MostPopularRanking Repository represents a mapping from publisher to ranking
 */
public class MostPopularRankingRepository implements RankingRepository,Serializable {
  //Maps a publisher to his item rankings
  private Map<String, MostPopularRanking> rankings;

  public MostPopularRankingRepository() {
    this(new HashMap<>());
  }

  public MostPopularRankingRepository(Map<String, MostPopularRanking> rankings) {
    this.rankings = rankings;
  }

  @Override
  public Optional<MostPopularRanking> getRanking(String key) {
    return Optional.ofNullable(rankings.get(key));
  }

  @Override
  public Map<String, MostPopularRanking> getRankings() {
    return rankings;
  }

  @Override
  public void merge(MostPopularRankingRepository repository) {
    merge(rankings, repository.getRankings());
  }

  private void merge(Map<String, MostPopularRanking> mergedRankings, Map<String, MostPopularRanking> rankings) {
    for (String publisher : rankings.keySet()) {
      mergedRankings.putIfAbsent(publisher, new MostPopularRanking());
      MostPopularRanking mostPopularRanking = mergedRankings.get(publisher);
      mostPopularRanking.merge(rankings.get(publisher));
    }
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    rankings.forEach((publisher, ranking) -> {
      result
          .append("\nPublisher: ")
          .append(publisher)
          .append('\n');
      ranking.getRanking().forEach((item, count) -> result
          .append(item)
          .append(" (").append(count).append(")\n"));
    });
    return result.toString();
  }

  @Override
  public void sortRankings() {
    rankings.forEach((publisher, ranking) -> ranking.sort());
  }
}
