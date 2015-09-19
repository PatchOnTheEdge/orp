package de.tuberlin.orp.master;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.message.OrpItemUpdate;
import scala.collection.Seq;
import scala.concurrent.duration.Duration;

import java.security.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Patch on 31.08.2015.
 */
public class ItemHandler extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  private HashMap<String,OrpItemUpdate> items;
  private HashMap<Date,String> creationTime;


  public HashMap<String, OrpItemUpdate> getItems() {
    return items;
  }
  public OrpItemUpdate getItemById(String itemId){
    return items.get(itemId);
  }
  @Override
  public void preStart(){
    items = new HashMap<>();
    creationTime = new HashMap<>();

    //Every Hour: clean items older than 2 days;
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(1, TimeUnit.HOURS), () -> {

      Calendar currentCalendar = Calendar.getInstance();
      currentCalendar.add(Calendar.DATE,-2);
      Date time = currentCalendar.getTime();
      creationTime.keySet().stream().filter(date -> date.before(time)).forEach(date -> {
        items.remove(creationTime.get(date));
      });
    }, getContext().dispatcher());
  }
  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpItemUpdate){

      OrpItemUpdate item = (OrpItemUpdate) message;
      String itemId = item.getItemId();
      log.info("Received Item with ID = " + itemId);

      //TODO check for doubles?
      items.put(itemId, item);
      creationTime.put(new Date(),itemId);
    }
    else if(message.equals("getItems")){
      getSender().tell(getItems(),getSelf());
      //log.info("Sending items. Nr.: " + getItems().size());
    }
  }


  public static Props create() { return Props.create(ItemHandler.class, new ItemHandlerCreator());}

  private static class ItemHandlerCreator implements Creator<ItemHandler> {
    @Override
    public ItemHandler create() throws Exception {
      return new ItemHandler();
    }
  }
}
