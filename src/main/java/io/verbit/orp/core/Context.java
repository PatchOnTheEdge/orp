package io.verbit.orp.core;

import com.fasterxml.jackson.databind.JsonNode;

public class Context {
  private String publisherId;
  private String itemId;
  private String userId;
  private int limit;

  public Context() {
  }

  public Context(JsonNode jsonNode) {
    this.publisherId = jsonNode.at("/context/simple/27").asText();
    this.itemId = jsonNode.at("/context/simple/25").asText();
    this.userId = jsonNode.at("/context/simple/57").asText();
    this.limit = jsonNode.path("limit").asInt(20);
  }

  public String getPublisherId() {
    return publisherId;
  }

  public String getItemId() {
    return itemId;
  }

  public String getUserId() {
    return userId;
  }

  public int getLimit() {
    return limit;
  }
}
