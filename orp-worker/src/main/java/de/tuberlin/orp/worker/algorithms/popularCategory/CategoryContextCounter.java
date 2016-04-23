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

package de.tuberlin.orp.worker.algorithms.popularCategory;

import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.ranking.PopularCategoryRanking;
import de.tuberlin.orp.common.ranking.Ranking;
import de.tuberlin.orp.common.repository.RankingRepository;

import java.util.*;
import java.util.stream.Collectors;

public class CategoryContextCounter {
  private int contextWindowSize;
  private Map<String, ArrayDeque<OrpContext>> contextWindows;
  private int topListSize;

  public CategoryContextCounter(int contextWindowSize, int topListSize) {
    this.contextWindowSize = contextWindowSize;
    this.contextWindows = new HashMap<>();
    this.topListSize = topListSize;
  }


  public void add(OrpContext context) {
    contextWindows.putIfAbsent(context.getPublisherId(), new ArrayDeque<>(contextWindowSize));
    ArrayDeque<OrpContext> contextWindow = contextWindows.get(context.getPublisherId());

    if (contextWindow.size() >= contextWindowSize) {
      contextWindow.removeFirst();
    }
    contextWindow.addLast(context);
  }

  public RankingRepository getRankingRepository() {
    Map<String, Map<String, Long>> countMap = calculateRankings();

    Map<String, Ranking> rankings = new HashMap<>(countMap.size());
    for (Map.Entry<String, Map<String, Long>> entry : countMap.entrySet()) {
      String publisher = entry.getKey();
      PopularCategoryRanking ranking = new PopularCategoryRanking(entry.getValue());
      ranking.sort();
      ranking.slice(topListSize);
      rankings.put(publisher, ranking);
    }

    return new RankingRepository(rankings, new PopularCategoryRanking());
  }

  private Map<String, Map<String, Long>> calculateRankings() {
    Map<String, Map<String, Long>> rankings = new HashMap<>();

    for (Map.Entry<String, ArrayDeque<OrpContext>> contextWindow : contextWindows.entrySet()) {
      Map<String, Long> itemRankings = contextWindow.getValue().stream().collect(Collectors.groupingBy(OrpContext::getItemId, Collectors.counting()));
      Map<String, Set<String>> categories = new HashMap<>();

      for (OrpContext orpContext : contextWindow.getValue()) {

        for (String category : orpContext.getCategory()) {
          Set<String> items = categories.getOrDefault(category, new HashSet<>());
          items.add(orpContext.getItemId());
          categories.put(category, items);
        }
      }

      Map<String, Long> categoryRank = new HashMap<>();

      for (Map.Entry<String, Set<String>> categoryItemMap : categories.entrySet()) {
        Long rank = categoryRank.getOrDefault(categoryItemMap.getValue(), 0L);
        for (String item : categoryItemMap.getValue()) {
          rank += itemRankings.get(item);
        }
        categoryRank.put(categoryItemMap.getKey(), rank);
      }

      String mpCategory = Collections.max(categoryRank.entrySet(), (entry1, entry2) -> entry1.getValue() > entry2.getValue() ? 1 : -1).getKey();
      Map<String, Long> popularItems = new HashMap<>();

      for (String item : categories.get(mpCategory)) {
        popularItems.put(item, itemRankings.get(item));
      }

      rankings.put(contextWindow.getKey(), popularItems);
    }
    return rankings;
  }
}

