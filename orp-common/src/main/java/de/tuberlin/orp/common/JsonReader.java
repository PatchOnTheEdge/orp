package de.tuberlin.orp.common;

import com.fasterxml.jackson.databind.JsonNode;
import io.verbit.ski.core.json.Json;
import org.json.JSONException;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
/**
 * Created by Patch on 23.09.2015.
 */
public class JsonReader {

  private static String readAll(Reader rd) throws IOException {
    StringBuilder sb = new StringBuilder();
    int cp;
    while ((cp = rd.read()) != -1) {
      sb.append((char) cp);
    }
    return sb.toString();
  }

  public static JsonNode readJsonFromUrl(String url) throws IOException, JSONException {
    try (InputStream is = new URL(url).openStream()) {
      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
      String jsonText = readAll(rd);
      return Json.parse(jsonText);
    }
  }
}
