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

import de.tuberlin.orp.common.message.OrpContext;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RankingFilter implements Serializable {
  private Set<String> removed;
  private Map<String, Set<String>> recommended;

  public RankingFilter(Set<String> removed, Map<String, Set<String>> recommended) {
    this.removed = removed;
    this.recommended = recommended;
  }

  public RankingFilter() {
    this(new HashSet<>(), new HashMap<>());
  }

  public void merge(RankingFilter filter) {
    removed.addAll(filter.getRemoved());
    filter.getRecommended().forEach((key, value) -> {
      recommended.putIfAbsent(key, new HashSet<>());
      Set<String> recs = recommended.get(key);
      recs.addAll(value);
    });
  }

  public Ranking filter(Ranking toFilter, OrpContext context) {
    Set<String> toRemove = new HashSet<>();
    toRemove.addAll(recommended.getOrDefault(context.getUserId(), Collections.emptySet()));
    toRemove.addAll(removed);

    toFilter.filter(toRemove);
    toFilter.slice(context.getLimit());

    return toFilter;
  }

  public Set<String> getRemoved() {
    return removed;
  }

  public Map<String, Set<String>> getRecommended() {
    return recommended;
  }
}
