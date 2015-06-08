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
import de.tuberlin.orp.core.Context;
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
