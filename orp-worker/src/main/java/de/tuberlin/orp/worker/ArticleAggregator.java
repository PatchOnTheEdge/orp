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

import java.util.*;

/**
 * Created by patch on 10.11.2015.
 */
public class ArticleAggregator extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private ActorSelection articleMerger;
  private ArticleRepository articles;
  private ArrayDeque<OrpArticle> newArticles;
  private HashSet<OrpArticleRemove> removedArticles;

  public ArticleAggregator(ActorSelection articleMerger) {
    this.articleMerger = articleMerger;
    this.articles = new ArticleRepository();
    this.newArticles = new ArrayDeque<>();
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

      ArticleMerger.ArticleAggregatorResult articleAggregatorResult = new ArticleMerger.ArticleAggregatorResult(this.newArticles, this.removedArticles);
      getSender().tell(articleAggregatorResult, getSelf());

      this.articles = ((ArticleMerger.MergedArticles) message).getArticles();
      this.newArticles = new ArrayDeque<>();
      this.removedArticles = new HashSet<>();

    } else {
      unhandled(message);
    }


  }
}
