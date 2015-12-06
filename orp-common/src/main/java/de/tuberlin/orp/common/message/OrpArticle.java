package de.tuberlin.orp.common.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.verbit.ski.core.json.Json;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by patch on 10.11.2015.
 */
public class OrpArticle implements Serializable{
  private String itemId;
  private String title;
  private String text;
  private String articleURL;
  private String imgURL;
  private String publisherId;
  private Date date;

  public OrpArticle(JsonNode json) {
    this.itemId = json.get("id").asText();
    this.title = json.get("title").asText();
    this.text = json.get("text").asText();
    this.articleURL = json.get("url").asText();
    this.imgURL = json.get("img").asText();
    this.publisherId = json.get("domainid").asText();
    this.date = new Date();
  }
  public OrpArticle(String itemId, String publisherId){
    this.itemId = itemId;
    this.publisherId = publisherId;
    this.title = "unknown";
    this.text = "unknown";
    this.articleURL  = "";
    this.imgURL = "";
    this.date = new Date();
  }

  public String getTitle() {
    return title;
  }

  public String getItemId() {
    return itemId;
  }

  public String getText() {
    return text;
  }

  public String getArticleURL() {
    return articleURL;
  }

  public String getImgURL() {
    return imgURL;
  }

  public String getPublisherId() {
    return publisherId;
  }

  public Date getDate() {
    return date;
  }

  public ObjectNode getJson(){
    return Json.newObject()
        .put("itemId", itemId)
        .put("publisherId", publisherId)
        .put("title", title)
        .put("text", text)
        .put("articleUrl", articleURL)
        .put("imgUrl", imgURL);
  }


}
