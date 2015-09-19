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

package de.tuberlin.orp.common.message;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.verbit.ski.core.json.Json;

import java.io.Serializable;

public class OrpItemUpdate implements Serializable {
  private String itemId;
  private String title;
  private String text;
  private String articleURL;
  private String imgURL;
  private int flag;

  public OrpItemUpdate() {
  }

  public OrpItemUpdate(String itemId, int flag) {
    this.itemId = itemId;
    this.flag = flag;
  }

  public OrpItemUpdate(String itemId, String title, int flag) {
    this.itemId = itemId;
    this.title = title;
    this.flag = flag;
  }

  /**
   *
   * @param itemId
   * @param title
   * @param text
   * @param articleURL
   * @param imgURL
   * @param flag
   */
  public OrpItemUpdate(String itemId, String title, String text, String articleURL, String imgURL, int flag) {
    this.itemId = itemId;
    this.title = title;
    this.text = text;
    this.articleURL = articleURL;
    this.imgURL = imgURL;
    this.flag = flag;
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

  public ObjectNode getJson(){
    ObjectNode node = Json.newObject();
    //node.put("itemId", itemId);
    node.put("title", title);
    node.put("text", text);
    node.put("articleUrl", articleURL);
    node.put("imgUrl", imgURL);
    node.put("flag",flag);
    return node;
  }
  public boolean isItemRecommendable() {
    return (flag & 1) == 1;
  }
}
