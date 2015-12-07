package de.tuberlin.orp.worker.algorithms.popularityTrend;

import de.tuberlin.orp.common.message.OrpContext;
import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.ranking.Ranking;
import de.tuberlin.orp.common.repository.RankingRepository;
import de.tuberlin.orp.worker.algorithms.mostPopular.OrpContextCounter;
import org.apache.commons.math.stat.regression.SimpleRegression;

import java.util.*;

/**
 * Created by Patch on 06.12.2015.
 */
public class TrendRepository {
  private int counterSize;
  private int windowSize;
  private int topListSize;
  private int intervallMinutes;
  private OrpContextCounter counter;
  private ArrayDeque<OrpContextCounter> counterRepo;

  public TrendRepository(int counterSize, int windowSize, int topListSize, int intervallMinutes) {
    this.counterSize = counterSize;
    this.windowSize = windowSize;
    this.topListSize = topListSize;
    this.intervallMinutes = intervallMinutes;
    this.counter = new OrpContextCounter(counterSize, topListSize);
    this.counterRepo = new ArrayDeque<>(windowSize);
  }

  public void add(OrpContext context) {
    counter.add(context);
  }

  public void newInterval() {
    if (counterRepo.size() >= windowSize){
      counterRepo.removeFirst();
    }
    counterRepo.add(counter);
  }
  public RankingRepository getRankingRespository() {
    Map<String, Map<String, Long>> countMap = calculateRankings();

    Map<String, Ranking> rankings = new HashMap<>(countMap.size());
    for (Map.Entry<String, Map<String, Long>> entry : countMap.entrySet()) {
      String key = entry.getKey();
      MostPopularRanking value = new MostPopularRanking(entry.getValue());
      value.sort();
      value.slice(topListSize);
      rankings.put(key, value);
    }

    return new RankingRepository(rankings, new MostPopularRanking());
  }

  private Map<String, Map<String, SimpleRegression>> calculateRegression(){
    Map<String, Map<String, SimpleRegression>> result = new LinkedHashMap<>();

    int x = 0;

    for (OrpContextCounter orpContextCounter : this.counterRepo) {
      Map<String, Ranking> rankings = orpContextCounter.getRankingRespository().getRankings();

      for (String publisher : rankings.keySet()) {
        Map<String, SimpleRegression> idRegressionMap = result.getOrDefault(publisher, new LinkedHashMap<>());
        MostPopularRanking ranking = (MostPopularRanking) rankings.get(publisher);
        LinkedHashMap<String, Long> rankMap = ranking.getRanking();

        for (String id : rankMap.keySet()) {
          SimpleRegression reg = idRegressionMap.getOrDefault(id, new SimpleRegression());
          reg.addData(x, rankMap.get(id));
          idRegressionMap.put(id, reg);

        }
        result.put(publisher, idRegressionMap);
      }
      x += this.intervallMinutes;
    }
    return result;
  }

  private Map<String, Map<String, Long>> calculateRankings() {
    Map<String, Map<String, Long>> result = new LinkedHashMap<>();
    Map<String, Map<String, SimpleRegression>> regressions = calculateRegression();

    for (String pub : regressions.keySet()) {

      Map<String, Long> rankings = result.getOrDefault(pub, new LinkedHashMap<>());

      for (String id : regressions.get(pub).keySet()) {
        rankings.put(id, (long) regressions.get(pub).get(id).getSlope());
      }
      result.put(pub, rankings);
    }
    return result;
  }
}


