package de.tuberlin.orp.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import akka.routing.Broadcast;
import akka.routing.FromConfig;
import de.tuberlin.orp.common.message.OrpArticle;
import de.tuberlin.orp.common.repository.ArticleRepository;
import de.tuberlin.orp.common.message.OrpArticleRemove;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by Patch on 31.08.2015.
 * This Actor holds Items.
 * Items information are needed by the GUI to show context information.
 * They are stored for some days before discarded.
 */
public class ArticleMerger extends UntypedActor {

  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private ActorRef workerRouter;

  private ArticleRepository articles;

  public ArticleMerger(int storageDays) {
    this.articles = new ArticleRepository();
  }

  /**
   * This method schedules the cleaning of the item storage (publisherItemIdMap)
   * The map is cleaned once every hour.
   * Items older than itemStorageDays will be removed.
   * @throws Exception
   */
  @Override
  public void preStart() throws Exception {
    log.info("OrpArticle Handler started.");

    workerRouter = getContext().actorOf(FromConfig.getInstance().props(Props.empty()), "workerRouter");

    // asks every 2 seconds for the intermediate ranking
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(1, TimeUnit.SECONDS), () -> {

      workerRouter.tell(new Broadcast(new MergedArticles(articles)), getSelf());
//      articles = new ArticleRepository();

    }, getContext().dispatcher());


    int itemStorageDays = 2;

    //Every Hour: clean items older than itemStorageDays;
    getContext().system().scheduler().schedule(Duration.create(1, TimeUnit.HOURS), Duration.create(1, TimeUnit.HOURS), () -> {

      log.info("Deleting items older than " + itemStorageDays + " days.");
      articles.clean(itemStorageDays);

    }, getContext().dispatcher());

    super.preStart();
  }

  /**
   * Handles received messages.
   * @param message A message can either be an item (OrpArticleRemove) or "getItems" to request all stored items
   * or "getRecentItems" to get only the last recently added items.
   */
  @Override
  public void onReceive(Object message) {

    if(message instanceof NewArticles){

      NewArticles articles = (NewArticles) message;
      HashSet<OrpArticle> newArticles = articles.getNewArticles();
      this.articles.merge(newArticles);

    } else if (message instanceof RemovedArticles) {

      RemovedArticles articles = (RemovedArticles) message;
      HashSet<OrpArticleRemove> removedArticles = articles.getRemovedArticles();
      this.articles.remove(removedArticles);

    } else if (message.equals("getArticles")) {

      MergedArticles articles = new MergedArticles(this.articles);
      getSender().tell(articles, getSelf());

    } else {
      unhandled(message);
    }
  }


  public static Props create() { return Props.create(ArticleMerger.class, new ItemHandlerCreator());}

  private static class ItemHandlerCreator implements Creator<ArticleMerger> {
    @Override
    public ArticleMerger create() throws Exception {
      return new ArticleMerger(2);
    }
  }

  public static class MergedArticles implements Serializable{
    private ArticleRepository articles;

    public MergedArticles(ArticleRepository articles) {
      this.articles = articles;
    }

    public ArticleRepository getArticles() {
      return articles;
    }
  }

  public static class NewArticles implements Serializable{
    private HashSet<OrpArticle> newArticles;

    public NewArticles(HashSet<OrpArticle> newArticles) {
      this.newArticles = newArticles;
    }

    public HashSet<OrpArticle> getNewArticles() {
      return newArticles;
    }
  }
  public static class RemovedArticles implements Serializable{
    private HashSet<OrpArticleRemove> removedArticles;

    public RemovedArticles(HashSet<OrpArticleRemove> removedArticles) {
      this.removedArticles = removedArticles;
    }

    public HashSet<OrpArticleRemove> getRemovedArticles() {
      return removedArticles;
    }
  }

}
