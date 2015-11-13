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

package de.tuberlin.orp.common;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.tuberlin.orp.common.message.OrpArticleRemove;
import io.verbit.ski.core.json.Json;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {
  public static <K, V> LinkedHashMap<K, V> sortMapByEntry(Map<K, V> unsortedMap,
      Comparator<Map.Entry<K, V>> comparator) {
    return unsortedMap.entrySet().stream()
        .sorted(comparator)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o, o2) -> o, LinkedHashMap::new));
  }

  public static <K, V> LinkedHashMap<K, V> sliceMap(LinkedHashMap<K, V> map, int n) {
    return map.entrySet().stream()
        .limit(n)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o, o2) -> o, LinkedHashMap::new));
  }
  //TODO beautify and change deprecated methods
  public static ObjectNode itemMapAsJson(Map<String, Map<String, OrpArticleRemove>> map){
    ObjectNode result = Json.newObject();
    ArrayNode resultArray = result.putArray("items");

    for (String publisher : map.keySet()) {
      Map<String, OrpArticleRemove> items = map.get(publisher);
      for (String itemId : items.keySet()) {
        OrpArticleRemove item = items.get(itemId);
        ObjectNode itemJson = Json.newObject();
        ObjectNode node = item.getJson();
        itemJson.put("itemId", itemId);
        itemJson.put("item", node);
        resultArray.add(itemJson);
      }
    }
    return result;
  }

}
