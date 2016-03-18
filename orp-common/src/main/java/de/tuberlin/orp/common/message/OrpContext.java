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

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

public class OrpContext implements Serializable {
  private String publisherId;
  private String itemId;
  private String userId;
  private String category;
  private int limit;

  public OrpContext() {
  }

  public OrpContext(JsonNode jsonNode) {
    System.out.println(jsonNode);
    this.publisherId = jsonNode.at("/context/simple/27").asText();
    this.itemId = jsonNode.at("/context/simple/25").asText();
    this.userId = jsonNode.at("/context/simple/57").asText();
    this.category = jsonNode.at("/context/lists/11/0").asText();
    this.limit = jsonNode.get("limit").asInt(20);
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

  public String getCategory() {
    return category;
  }


  public int getLimit() {
    return limit;
  }


}
