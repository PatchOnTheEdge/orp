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

package de.tuberlin.orp.common.repository;

import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.ranking.Ranking;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A Ranking Repository represents a mapping from publisher to ranking
 */
public class RankingRepository  implements Serializable{
  //Maps a publisher to his item ranking
  private Map<String, Ranking> rankings;
  private Ranking type;

  public RankingRepository(Ranking type) {
    this.rankings = new HashMap<>();
    this.type = type;
  }

  public RankingRepository(Map<String, Ranking> rankings, Ranking type) {
    this.rankings = rankings;
    this.type = type;
  }

  public Optional<Ranking> getRanking(String key, int limit) {
    Ranking ranking = rankings.get(key);
    ranking.slice(limit);
    return Optional.ofNullable(ranking);
  }

  public Map<String, Ranking> getRankings() {
    return rankings;
  }

  public void merge(RankingRepository repository) {
    merge(rankings, repository.getRankings());
  }

  private void merge(Map<String, Ranking> mergedRankings, Map<String, Ranking> rankings) {
    if (this.type instanceof MostPopularRanking){
      for (String publisher : rankings.keySet()) {
        mergedRankings.putIfAbsent(publisher, new MostPopularRanking());
        Ranking ranking = mergedRankings.get(publisher);
        ranking.merge(rankings.get(publisher));
      }
//    } else if (this.type instanceof MostRecentRanking){
//      for (String publisher : rankings.keySet()) {
//        mergedRankings.putIfAbsent(publisher, new MostRecentRanking());
//        Ranking ranking = mergedRankings.get(publisher);
//        ranking.merge(rankings.get(publisher));
//      }
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
  public void sortRankings() {
    rankings.forEach((publisher, ranking) -> ranking.sort());
  }

}
