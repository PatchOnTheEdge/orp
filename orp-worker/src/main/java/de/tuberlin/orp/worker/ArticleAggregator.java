package de.tuberlin.orp.worker;

import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.tuberlin.orp.common.repository.ArticleRepository;
import de.tuberlin.orp.common.message.OrpArticle;
import de.tuberlin.orp.common.message.OrpArticleRemove;
import de.tuberlin.orp.master.ArticleMerger;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by patch on 10.11.2015.
 */
public class ArticleAggregator extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private ActorSelection articleMerger;
  private ArticleRepository articles;
  private HashSet<OrpArticle> newArticles;
  private HashSet<OrpArticleRemove> removedArticles;

  public ArticleAggregator(ActorSelection articleMerger) {
    this.articleMerger = articleMerger;
    this.articles = new ArticleRepository();
    this.newArticles = new HashSet<>();
    this.removedArticles = new HashSet<>();
  }
  public static Props create(ActorSelection articleMerger) {
    return Props.create(ArticleAggregator.class, () -> {
      return new ArticleAggregator(articleMerger);
    });
  }

  @Override
  public void preStart() throws Exception {
    log.info("Article Aggregator started");

    getContext().system().scheduler().schedule(Duration.create(10, TimeUnit.SECONDS), Duration.create(30, TimeUnit.SECONDS), () -> {

//      articles.storeCategories();

    }, getContext().dispatcher());

    super.preStart();
  }

  @Override
  public void onReceive(Object message) throws Exception {

    if (message instanceof OrpArticleRemove){

      removedArticles.add((OrpArticleRemove) message);

    } else if (message instanceof OrpArticle){

      newArticles.add((OrpArticle) message);

    } else if(message.equals("getItems")){

      getSender().tell(articles.getArticles(), getSelf());
      log.info("Sending items.");

    } else if (message instanceof ArticleMerger.MergedArticles){

      ArticleMerger.NewArticles msg1 = new ArticleMerger.NewArticles(this.newArticles);
      ArticleMerger.RemovedArticles msg2 = new ArticleMerger.RemovedArticles(this.removedArticles);

//      log.debug("new Articles = " + newArticles.size() + ". removed = " + removedArticles.size());
      getSender().tell(msg1, getSelf());
      getSender().tell(msg2, getSelf());

      this.articles = ((ArticleMerger.MergedArticles) message).getArticles();
      this.newArticles = new HashSet<>();
      this.removedArticles = new HashSet<>();

    } else if (message instanceof ArticleCategory){

      ArticleCategory categoryMessage = (ArticleCategory) message;
//      articles.setCategory(categoryMessage.publisherId, categoryMessage.itemId, categoryMessage.category);

    } else {
      unhandled(message);
    }
  }

  public static class ArticleCategory implements Serializable{
    private String publisherId;
    private String itemId;
    private String category;
    public ArticleCategory(String publisherId, String itemId, String category) {
      this.publisherId = publisherId;
      this.itemId = itemId;
      this.category = category;
    }
  }
}
