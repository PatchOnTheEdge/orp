package de.tuberlin.orp.common.repository;

import com.fasterxml.jackson.databind.JsonNode;
import de.tuberlin.orp.common.JsonReader;
import de.tuberlin.orp.common.LiFoRingBuffer;
import de.tuberlin.orp.common.message.OrpArticle;
import de.tuberlin.orp.common.message.OrpArticleRemove;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.*;

/**
 * Created by patch on 10.11.2015.
 */
public class ArticleRepository implements Serializable{

  //Map from PublisherId -> ItemId -> OrpArticle;
  private final Map<String, Map<String, OrpArticle>> publisherItemIdMap;

  public ArticleRepository() {
    this.publisherItemIdMap = new HashMap<>();
  }

  public void clean(int itemStorageDays){
    //Get maximum age for items
    Long time = Instant.now().getEpochSecond();
    HashMap<String, Map<String, OrpArticle>> copyMap = new HashMap<>(publisherItemIdMap);


    //Delete items older than maximum age
    for (String publisherId : copyMap.keySet()) {
      Map<String, OrpArticle> items = copyMap.get(publisherId);
      HashSet<String> toRemove = new HashSet<>();
      for (String id : items.keySet()) {
        if (time - items.get(id).getDate() < 86400 * itemStorageDays) {
          toRemove.add(id);
        }
      }
      items.remove(toRemove);
      copyMap.put(publisherId, items);
    }
    publisherItemIdMap.putAll(copyMap);
  }

  public void merge(HashSet<OrpArticle> newArticles) {
    for (OrpArticle newArticle : newArticles) {
      add(newArticle);
    }
  }

  public void add(OrpArticle article){
    String itemId = article.getItemId();
    String publisherId = article.getPublisherId();

    publisherItemIdMap.putIfAbsent(publisherId, new HashMap<>());
    Map<String, OrpArticle> articleMap = publisherItemIdMap.get(publisherId);
    articleMap.put(itemId, article);
  }


  public Map<String, Map<String, OrpArticle>> getArticles() {
    return publisherItemIdMap;
  }

  public void remove(Set<OrpArticleRemove> removedArticles) {
    removedArticles.forEach(this::remove);
  }

  public void remove(OrpArticleRemove toRemove) {
    String itemId = toRemove.getItemId();
    String publisherId = toRemove.getPublisherId();
    Map<String, OrpArticle> articleMap = publisherItemIdMap.get(publisherId);
    if (articleMap != null){
      articleMap.remove(itemId);
    }
  }

}
