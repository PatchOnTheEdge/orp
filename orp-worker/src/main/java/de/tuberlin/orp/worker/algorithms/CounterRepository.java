package de.tuberlin.orp.worker.algorithms;

import de.tuberlin.orp.common.ranking.MostPopularRanking;
import de.tuberlin.orp.common.ranking.Ranking;
import de.tuberlin.orp.common.repository.RankingRepository;
import org.apache.commons.math.stat.regression.SimpleRegression;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Patch on 06.12.2015.
 */
public class CounterRepository {
  private int windowSize;
  private int topListSize;
  private ArrayDeque<OrpContextCounter> counterRepo;

  public CounterRepository(int windowSize, int topListSize) {
    this.windowSize = windowSize;
    this.topListSize = topListSize;
    this.counterRepo = new ArrayDeque<>(windowSize);
  }

  public void add(OrpContextCounter contextCounter) {
    if (counterRepo.size() >= windowSize) {
      counterRepo.removeFirst();
    }

    counterRepo.add(contextCounter);
  }

//  public RankingRepository getRankingRespository() {
//    Map<String, Map<String, Long>> countMap = calculateRankings();
//
//    Map<String, Ranking> rankings = new HashMap<>(countMap.size());
//    for (Map.Entry<String, Map<String, Long>> entry : countMap.entrySet()) {
//      String key = entry.getKey();
//      MostPopularRanking value = new MostPopularRanking(entry.getValue());
//      value.sort();
//      value.slice(topListSize);
//      rankings.put(key, value);
//    }
//
//    return new RankingRepository(rankings, new MostPopularRanking());
//  }
  private Map<String, Map<String, SimpleRegression>> calculateRankings() {

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
      x += 30;
    }
    return result;
  }
}
//  private Map<String, Map<String, Long>> calculateRankings() {
//    return counterRepo.stream()
//        .collect(
//            Collectors.groupingBy(
//                OrpContext::getPublisherId,
//                Collectors.groupingBy(OrpContext::getItemId, Collectors.counting())
//            ));
//  }

