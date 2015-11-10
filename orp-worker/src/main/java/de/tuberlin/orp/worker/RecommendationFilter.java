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

package de.tuberlin.orp.worker;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.ranking.RankingFilter;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RecommendationFilter extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  //todo is removed exploding?
  private Set<String> removed;
  private Map<String, Long> lastUpdated;
  private Map<String, Set<String>> recommended;

  public static Props create() {
    return Props.create(RecommendationFilter.class, new RecommendationFilterCreator());
  }

  public RecommendationFilter() {
    removed = new HashSet<>();
    lastUpdated = new HashMap<>();
    recommended = new HashMap<>();
  }

  public static class Removed implements Serializable {
    private String itemId;

    public Removed() {
    }

    public Removed(String itemId) {
      this.itemId = itemId;
    }

    public String getItemId() {
      return itemId;
    }
  }

  public static class Clicked implements Serializable {
    private String userId;
    private String itemId;

    public Clicked() {
    }

    public Clicked(String userId, String itemId) {
      this.userId = userId;
      this.itemId = itemId;
    }

    public String getUserId() {
      return userId;
    }

    public String getItemId() {
      return itemId;
    }
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    log.info("Recommendation filter started");
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(1, TimeUnit.MINUTES), () -> {
      cleanRecommended();
    }, getContext().dispatcher());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof Removed) {

      removed.add(((Removed) message).getItemId());

    } else if (message instanceof Clicked) {

      Clicked clicked = (Clicked) message;
      recommended.putIfAbsent(clicked.getUserId(), new HashSet<>());
      Set<String> itemsRecommended = recommended.get(clicked.getUserId());
      itemsRecommended.add(clicked.getItemId());

      lastUpdated.put(clicked.getUserId(), System.currentTimeMillis());

    } else if (message.equals("getIntermediateFilter")) {

      getSender().tell(new RequestCoordinator.IntermediateFilter(new RankingFilter(removed, recommended)), getSelf());

    } else {
      unhandled(message);
    }
  }

  /**
   * For a fixed time period recommended items for users are remembered in a map. Such a map will be cleaned so that its
   * size won't explode.
   */
  private void cleanRecommended() {
    long now = System.currentTimeMillis();
    Set<String> toRemove = new HashSet<>();
    for (String key : lastUpdated.keySet()) {
      long userLastUpdated = lastUpdated.get(key);
      if (now - userLastUpdated > 1000 * 60 * 30) {
        toRemove.add(key);
      }
    }
    for (String key : toRemove) {
      lastUpdated.remove(key);
      recommended.remove(key);
      removed.remove(key); //todo sensefull
    }
  }

  private static class RecommendationFilterCreator implements Creator<RecommendationFilter> {
    @Override
    public RecommendationFilter create() throws Exception {
      return new RecommendationFilter();
    }
  }
}
