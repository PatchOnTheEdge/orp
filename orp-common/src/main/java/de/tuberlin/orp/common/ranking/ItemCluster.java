package de.tuberlin.orp.common.ranking;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Patch on 15.10.2015.
 */
public class ItemCluster implements Serializable{
  private Map<String, Set<String>> itemItemsMap;

  public ItemCluster() {
    this.itemItemsMap = new HashMap<>();
  }

  public Map<String, Set<String>> getItemItemsMap() {
    return itemItemsMap;
  }
}
