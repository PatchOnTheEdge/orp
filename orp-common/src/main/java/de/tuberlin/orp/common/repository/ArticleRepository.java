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

  //Buffer holding recently added Elements for each publisher
  private Map<String, LiFoRingBuffer> recentItemBuffer;

  public ArticleRepository() {
    this.publisherItemIdMap = new HashMap<>();
    this.recentItemBuffer = new HashMap<>();
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

    recentItemBuffer.putIfAbsent(publisherId, new LiFoRingBuffer(5));
    recentItemBuffer.get(publisherId).add(article);
  }

  public RecentArticles getRecentArticles(){
    RecentArticles result = new RecentArticles();

    for (String publisherId : publisherItemIdMap.keySet()) {
      ArrayDeque<Object> buffer = recentItemBuffer.get(publisherId).getBuffer();
      result.getPublisherItems().put(publisherId, buffer);
    }
    return result;
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
    recentItemBuffer.get(publisherId).getBuffer().remove(itemId);
  }



  public static class RecentArticles implements Serializable{
    private HashMap<String, ArrayDeque> publisherItems;

    public RecentArticles() {
      this.publisherItems = new HashMap<String, ArrayDeque>();
    }

    public HashMap<String, ArrayDeque> getPublisherItems() {
      return publisherItems;
    }
  }
}
