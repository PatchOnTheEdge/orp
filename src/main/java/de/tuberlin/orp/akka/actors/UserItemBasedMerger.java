/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Ilya Verbitskiy, Patrick Probst
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.tuberlin.orp.akka.actors;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import scala.concurrent.Future;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Merger Class for the User-Item-Based Recommendation Algorithm.
 * This Class holds a "Publisher -> User -> Item" Map that combines the maps from all UserItemBasedActors
 */
public class UserItemBasedMerger extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private HashMap<String, HashMap<String, List<String>>> publisherUserItemMap = new HashMap<>();

  /**
   * This Method can receive Get- and SetMessages.
   * When a SetMessage is received, the transmitted Map is merged with the local Map.
   * When a GetMessage is received, the Merger transmits his local map back to the sender.
   * @param message Either a GetMessage or a SetMessage
   * @throws Exception
   */
  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof SetMessage){
      log.info("Received Set Message.");

      HashMap<String, HashMap<String, List<String>>> setMap = ((SetMessage) message).getSetMap();
      publisherUserItemMap = merge(setMap);
    }
    else if (message instanceof GetMessage){
      log.info("Received Get Message.");
      String publisher = ((GetMessage) message).getPublisher();
      String userId = ((GetMessage) message).getUserId();
      int limit = ((GetMessage) message).getLimit();

      List<String> returnItems = getReturnItems(publisher, userId, limit);
    }
  }

  /**
   * Merge Function that can combine a new Publisher-User-Item Map
   * with the private Map that is held by the UserItemBasedMerger
   * @param setMap The new Map that should be merged with the old one
   * @return Merged Map
   */
  private HashMap<String, HashMap<String, List<String>>> merge(HashMap<String, HashMap<String, List<String>>> setMap){
    HashMap<String, HashMap<String, List<String>>> returnMap = new HashMap<>();
    Set<String> publishers = setMap.keySet();

    //Horrific Code Style
    for (String publisher : publishers) {
      HashMap<String, List<String>> userItemMap = publisherUserItemMap.getOrDefault(publisher, new HashMap<>());

      if (setMap.containsKey(publisher)){
        HashMap<String, List<String>> setUserItemMap = setMap.get(publisher);

        for (String userId : setUserItemMap.keySet()) {
          if (userItemMap.containsKey(userId)){
            List<String> itemIds = userItemMap.get(userId);
            List<String> newItemIds = setUserItemMap.get(userId);
            newItemIds.stream().filter(itemId -> !itemIds.contains(itemId)).forEach(itemIds::add);
            userItemMap.put(userId,itemIds);
          }
          else{
            userItemMap.put(userId,setUserItemMap.get(userId));
          }
        }
      }
      if (userItemMap != null){
        returnMap.put(publisher,userItemMap);
      }
    }

    return returnMap;
  }

  private List<String> getReturnItems(String publisher, String userId, int limit){

    HashMap<String, List<String>> userItemsMap = publisherUserItemMap.get(publisher);
    List<String> items = userItemsMap.get(userId);
    List<String> returnItems = new ArrayList<>(limit);

    //TODO In case no similar User can be found or filling items are needed - ask MostPopularMerger?
//      ActorSelection mostPopularMerger = getContext().actorSelection("/user/mpmerger");
//      Future<Object> ask = Patterns.ask(mostPopularMerger, new MostPopularMerger.GetMessage(publisher, limit), 100);
//      getSender().tell(ask,getSelf());

    if (!items.isEmpty()){
      //There are items available so we are looking for a similar user
      //Items from current user aren't necessary
      userItemsMap.remove(userId);
      for (String user : userItemsMap.keySet()) {
        List<String> otherItems = userItemsMap.get(user);
        if (returnItems.size() >= limit){break;}
        if (otherItems.size() > 1){
          //Iterate over all UserItems and find other Users that clicked the same Items
          items.stream().filter(otherItems::contains).forEach(item -> {
            //Remove current UserItem, we want all other Items
            otherItems.remove(item);
            //Iterate over all found items and add them
            otherItems.stream().filter(otherItem -> !returnItems.contains(otherItem) && returnItems.size() < limit).forEach(returnItems::add);
          });
        }
      }
      if (returnItems.size() < limit) {
        //Also need to fill here
      }
    }
    else{
      //No items available so what are we doing?
    }
    return returnItems;
  }
  /**
   * Set-Message from UserItemBasedMerger.
   * Used to send a "Publisher -> User -> Item" Map that should be merged with the Mergers own Map.
   */
  public static class SetMessage{
    private HashMap<String, HashMap<String, List<String>>> setMap;

    public HashMap<String, HashMap<String, List<String>>> getSetMap() {
      return this.setMap;
    }

    public SetMessage(HashMap<String, HashMap<String, List<String>>> setMap) {
      this.setMap = setMap;

    }
  }

  /**
   * Get-Message from UserItemBasedMerger.
   * Used to get the "Publisher -> User -> Item" Map kept by this Merger.
   */
  public static class GetMessage{
    private String publisher;
    private String userId;
    private int limit;

    public GetMessage(String publisher, String userId, int limit) {
      this.publisher = publisher;
      this.userId = userId;
      this.limit = limit;
    }

    public String getPublisher() {
      return publisher;
    }

    public String getUserId() {
      return userId;
    }

    public int getLimit() {
      return limit;
    }
  }
}
