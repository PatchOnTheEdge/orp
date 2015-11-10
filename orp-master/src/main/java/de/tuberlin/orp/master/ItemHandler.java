package de.tuberlin.orp.master;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import de.tuberlin.orp.common.LiFoRingBuffer;
import de.tuberlin.orp.common.message.OrpItemUpdate;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Patch on 31.08.2015.
 * This Actor holds Items.
 * Items information are needed by the GUI to show context information.
 * They are stored for some days before discarded.
 */
public class ItemHandler extends UntypedActor {
  private static int itemStorageDays = 5;
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  //Map from PublisherId -> ItemId -> Item;
  private Map<String, Map<String, OrpItemUpdate>> publisherItemIdMap;

   //Buffer holding recently added Elements for each publisher
  private Map<String, LiFoRingBuffer> recentItemBuffer;

  public ItemHandler() {
    publisherItemIdMap = new HashMap<>();
    recentItemBuffer = new HashMap<>();
  }

  /**
   * This method schedules the cleaning of the item storage (publisherItemIdMap)
   * The map is cleaned once every hour.
   * Items older than itemStorageDays will be removed.
   * @throws Exception
   */
  @Override
  public void preStart() throws Exception {
    log.info("Item Handler started.");

    //Every Hour: clean items older than itemStorageDays;
    //TODO confirm that this works
    getContext().system().scheduler().schedule(Duration.create(1, TimeUnit.HOURS), Duration.create(1, TimeUnit.HOURS), () -> {
      log.info("Deleting items older than " + itemStorageDays + " days.");

      //Get maximum age for items
      Calendar currentCalendar = Calendar.getInstance();
      currentCalendar.add(Calendar.DATE, -itemStorageDays);
      Date time = currentCalendar.getTime();

      //Delete items older than maximum age
      for (String publisherId : publisherItemIdMap.keySet()) {
        Map<String, OrpItemUpdate> items = publisherItemIdMap.get(publisherId);
        items.keySet().stream().filter(item ->
            items.get(item).getDate().before(time)).forEach(items::remove);
      }
    }, getContext().dispatcher());
    super.preStart();
  }

  /**
   * Handles received messages.
   * @param message A message can either be an item (OrpItemUpdate) or "getItems" to request all stored items
   * or "getRecentItems" to get only the last recently added items.
   */
  @Override
  public void onReceive(Object message) {
    //Store received items in Hashmap and buffer
    if (message instanceof OrpItemUpdate){
      OrpItemUpdate item = (OrpItemUpdate) message;
      String itemId = item.getItemId();
      String publisherId = item.getPublisherId();

//      log.info("Received Item (Pb = " + publisherId + ") with ID = " + itemId);

      publisherItemIdMap.putIfAbsent(publisherId, new HashMap<>());
      publisherItemIdMap.get(publisherId).put(itemId, item);

      recentItemBuffer.putIfAbsent(publisherId, new LiFoRingBuffer(5));
      recentItemBuffer.get(publisherId).add(item);
    }
    //Send all stored items
    else if(message.equals("getItems")){
      Map<String, Map<String, OrpItemUpdate>> items = getItems();
      getSender().tell(items, getSelf());
      log.info("Sending items." + items.keySet());
    }
    //Send last 5 items
    else if(message.equals("getRecentItems")){
      RecentItems result = new RecentItems();
      for (String publisherId : publisherItemIdMap.keySet()) {
        ArrayDeque<Object> buffer = recentItemBuffer.get(publisherId).getBuffer();
        result.getPublisherItems().put(publisherId, buffer);
      }
      getSender().tell(result, getSelf());
      log.info("Sending recent items");
    }
    else {
      unhandled(message);
    }
  }

  public Map<String, Map<String, OrpItemUpdate>> getItems() {
    return publisherItemIdMap;
  }

  public static Props create() { return Props.create(ItemHandler.class, new ItemHandlerCreator());}

  private static class ItemHandlerCreator implements Creator<ItemHandler> {
    @Override
    public ItemHandler create() throws Exception {
      return new ItemHandler();
    }
  }
  public static class RecentItems implements Serializable{
    private HashMap<String, ArrayDeque> publisherItems;

    public RecentItems() {
      this.publisherItems = new HashMap<String, ArrayDeque>();
    }

    public HashMap<String, ArrayDeque> getPublisherItems() {
      return publisherItems;
    }
  }
}
