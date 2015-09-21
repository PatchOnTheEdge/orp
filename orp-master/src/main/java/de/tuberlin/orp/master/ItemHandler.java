package de.tuberlin.orp.master;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.LiFoRingBuffer;
import de.tuberlin.orp.common.messages.OrpItemUpdate;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Patch on 31.08.2015.
 * This Actor holds Items.
 * Items information are needed by the GUI to show context information.
 * They are stored for some days before discarded.
 */
public class ItemHandler extends UntypedActor {
  private static int itemStorageDays = 2;
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  //Map from PublisherId -> ItemId -> Item;
  private Map<String, Map<String, OrpItemUpdate>> publisherItemIdMap;

  //Map from ItemId -> PublisherId
  private Map<String,String> itemPublisherMap;

  //Map from ItemId -> Time when item was added
  private Map<String, Date> creationTime;

  //Buffer holding recently added Elements for each publisher
  private Map<String, LiFoRingBuffer> recentItemBuffer;

  public ItemHandler() {
    publisherItemIdMap = new LinkedHashMap<>();
    itemPublisherMap = new HashMap<>();
    creationTime = new HashMap<>();
    recentItemBuffer = new HashMap<>();
  }

  public OrpItemUpdate[] getLast(String publisher, int n) {
    OrpItemUpdate[] lastN = (OrpItemUpdate[]) recentItemBuffer.get(publisher).getLastN(n);
    return lastN;
  }
  @Override
  public void preStart(){
    //Every Hour: clean items older than 2 days;
    getContext().system().scheduler().schedule(Duration.Zero(), Duration.create(1, TimeUnit.HOURS), () -> {
      Calendar currentCalendar = Calendar.getInstance();
      currentCalendar.add(Calendar.DATE, -itemStorageDays);
      Date time = currentCalendar.getTime();

      creationTime.keySet().stream().filter(itemId -> creationTime.get(itemId).before(time)).forEach(itemId -> {
        String publisherId = itemPublisherMap.get(itemId);
        Map<String, OrpItemUpdate> updateMap = publisherItemIdMap.get(publisherId);

        updateMap.remove(itemId);
        publisherItemIdMap.put(publisherId, updateMap);

        itemPublisherMap.remove(itemId);
        creationTime.remove(itemId);
      });
    }, getContext().dispatcher());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OrpItemUpdate){
      OrpItemUpdate item = (OrpItemUpdate) message;
      String itemId = item.getItemId();
      String publisherId = item.getPublisherId();

      log.info("Received Item with ID = " + itemId);

      publisherItemIdMap.getOrDefault(publisherId,new HashMap<>()).put(itemId,item);
      itemPublisherMap.put(itemId, publisherId);
      recentItemBuffer.getOrDefault(publisherId,new LiFoRingBuffer(6)).add(item);
      creationTime.put(itemId, new Date());
    }
    else if(message.equals("getItems")){
      getSender().tell(getItems(),getSelf());
      //log.info("Sending items. Nr.: " + getItems().size());
    }
  }

  public static Props create() { return Props.create(ItemHandler.class, new ItemHandlerCreator());}

  public Object getItems() {
    return publisherItemIdMap;
  }

  private static class ItemHandlerCreator implements Creator<ItemHandler> {
    @Override
    public ItemHandler create() throws Exception {
      return new ItemHandler();
    }
  }
}
