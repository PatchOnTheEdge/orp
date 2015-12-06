package de.tuberlin.orp.common.repository;

import de.tuberlin.orp.common.LiFoRingBuffer;
import de.tuberlin.orp.common.message.OrpArticle;
import de.tuberlin.orp.common.message.OrpArticleRemove;

import java.io.Serializable;
import java.util.*;

/**
 * Created by patch on 10.11.2015.
 */
public class ArticleRepository implements Serializable{
  //Map from PublisherId -> ItemId -> OrpArticle;
  private Map<String, Map<String, OrpArticle>> publisherItemIdMap;


  public ArticleRepository() {
    this.publisherItemIdMap = new HashMap<>();
  }

  public void clean(int itemStorageDays){
    //Get maximum age for items
    Calendar currentCalendar = Calendar.getInstance();
    currentCalendar.add(Calendar.DATE, -itemStorageDays);
    Date time = currentCalendar.getTime();

    //Delete items older than maximum age
    for (String publisherId : publisherItemIdMap.keySet()) {
      Map<String, OrpArticle> items = publisherItemIdMap.get(publisherId);
      items.keySet().stream().filter(item ->
          items.get(item).getDate().before(time)).forEach(items::remove);
    }
  }

  public void merge(ArrayDeque<OrpArticle> newArticles) {

    while(newArticles.iterator().hasNext()){
      add(newArticles.removeLast());
    }
  }

  public void add(OrpArticle article){
    String itemId = article.getItemId();
    String publisherId = article.getPublisherId();

    publisherItemIdMap.putIfAbsent(publisherId, new HashMap<>());
    publisherItemIdMap.get(publisherId).put(itemId, article);
  }


  public Map<String, Map<String, OrpArticle>> getArticles() {
    return publisherItemIdMap;
  }

  public void remove(Set<OrpArticleRemove> removedArticles) {
    removedArticles.forEach(this::remove);
  }

  public void remove(OrpArticleRemove toRemove){
    String itemId = toRemove.getItemId();
    String publisherId = toRemove.getPublisherId();
    publisherItemIdMap.get(publisherId).remove(itemId);
  }
}