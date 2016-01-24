package de.tuberlin.orp.common.repository;

import com.fasterxml.jackson.databind.JsonNode;
import de.tuberlin.orp.common.JsonReader;
import de.tuberlin.orp.common.message.OrpArticle;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Patch on 13.01.2016.
 */
public class CategoryRepository {
  // Map from CategoryId -> Category
  private Map<String, String> categoryMap;

  //Properties Object for storing the Category Map and static Filename
  private Properties categoryProperties;
  private static String CategoriesFilename = "categories.properties";

  public CategoryRepository() {
    this.categoryMap = new HashMap<>();
    this.categoryProperties = new Properties();

    //Load Retrieved Categories from file
    try {
      categoryProperties.load(new FileInputStream("orp-common/src/main/resources/categories.properties"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    for (String key : categoryProperties.stringPropertyNames()) {
      categoryMap.put(key, categoryProperties.get(key).toString());
    }
  }

  /**
   * Stores retrieved Categories to File
   */
  public void storeCategories(){
    categoryProperties.putAll(categoryMap);
    try {
      categoryProperties.store(new FileOutputStream("orp-common/src/main/resources/categories.properties"), null);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void setCategory(String categoryId) {

    String storedCategory = categoryMap.getOrDefault(categoryId, null);

    if (storedCategory == null){
      String category = requestCategoryString(categoryId);
      categoryMap.put(categoryId, category);
    }
  }

  private String requestCategoryString(String categoryId) {
    String url = "http://orp.plista.com/api/vector_resolution.php?vid=11&aid=" + categoryId;
    try {
      JsonNode json = JsonReader.readJsonFromUrl(url);
      return json.get("value_resolved").asText();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
