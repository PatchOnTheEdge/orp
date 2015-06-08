package io.verbit.orp.akka.actors;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import io.verbit.orp.core.Context;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class UserItemBasedActor extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private HashMap<String, HashMap<String, List<String>>> publisherUserItemMap = new HashMap<>();
  private final ActorSelection userItemBasedMerger = getContext().actorSelection("/user/useritembasedmerger");

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof Context) {
      Context context = (Context) message;
      String itemId = context.getItemId();
      String publisher = context.getPublisherId();
      String userId = context.getUserId();

      //Case: Traceable User
      if (!userId.equals("0")){
        log.info("Received Item. Publisher: " + publisher + ". Item ID: " + itemId + ". User ID: " + userId);

        HashMap<String, List<String>> userItemsMap = publisherUserItemMap.getOrDefault(publisher, new HashMap<>());
        List<String> itemIds = userItemsMap.getOrDefault(userId, new ArrayList<>());

        if (!itemIds.contains(itemId)){
          itemIds.add(itemId);
          userItemsMap.put(userId, itemIds);
          publisherUserItemMap.put(publisher, userItemsMap);
        }
      }
    }
  }
  public void preStart() throws Exception {
    super.preStart();

    //Number of merged most popular Entries
    int n = 10;

    getContext().system().scheduler().schedule(
        Duration.create(2, TimeUnit.SECONDS),
        Duration.create(2, TimeUnit.SECONDS), () -> {
//          log.info("Sending map = " + publisherUserItemMap);
          UserItemBasedMerger.SetMessage setMessage = new UserItemBasedMerger.SetMessage(publisherUserItemMap);
          userItemBasedMerger.tell(setMessage, getSelf());
        }, getContext().dispatcher());
  }
  public static class GetUser {

  }
}
