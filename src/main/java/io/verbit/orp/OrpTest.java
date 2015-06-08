/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Ilya Verbitskiy
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

package io.verbit.orp;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OrpTest {
  public static void main(String[] args) throws ExecutionException, InterruptedException, UnirestException, IOException {
    String json = "{\"recs\": {\"ints\": {\"3\": [186577395, 186144125, 186573977, 186543309, 186620846, " +
        "171259794]}}, \"event_type\": \"impression\", \"context\": {\"simple\": {\"62\": 1918108, \"63\": 1840689, " +
        "\"49\": 48, \"67\": 1928642, \"68\": 1851453, \"69\": 1851422, \"24\": 2, \"25\": 186578479, \"27\": 596, " +
        "\"22\": 62364, \"23\": 23, \"47\": 654013, \"44\": 1178658, \"42\": 0, \"29\": 17332, \"40\": 1618704, " +
        "\"5\": 889861, \"4\": 455421, \"7\": 18846, \"6\": 9, \"9\": 26890, \"13\": 2, \"76\": 1, \"75\": 1919903, " +
        "\"74\": 1919860, \"39\": 970, \"59\": 1275566, \"14\": 33331, \"17\": 48985, \"16\": 48811, \"19\": 52193, " +
        "\"18\": 6, \"57\": 4210813658, \"56\": 1138207, \"37\": 1978291, \"35\": 315003, \"52\": 1, \"31\": 0}, " +
        "\"clusters\": {\"46\": {\"761803\": 100, \"761813\": 100, \"472394\": 100}, \"51\": {\"2\": 255}, \"1\": " +
        "{\"7\": 255}, \"33\": {\"1700\": 0, \"622\": 0, \"19439\": 1, \"1935732\": 1, \"201157\": 2, \"2233004\": 3," +
        " \"2243325\": 2, \"17922945\": 6, \"313227\": 2, \"4785\": 1, \"5794\": 1, \"6107580\": 2, \"1592522\": 2, " +
        "\"10931\": 1, \"32941261\": 6, \"119035\": 1, \"80546\": 1}, \"3\": [50, 28, 34, 98, 28, 15], \"2\": [21, " +
        "11, 50, 74, 60, 23, 11], \"64\": {\"2\": 255}, \"65\": {\"1\": 255}, \"66\": {\"11\": 255}}, \"lists\": " +
        "{\"11\": [1243279], \"8\": [18841, 18842], \"10\": [4, 9, 1768, 1769, 1770]}}, \"timestamp\": 1404251999618}";
    JSONObject jsonObject = new JSONObject(json);


    HttpResponse<String> httpResponse = Unirest.post("http://localhost:9000/orp")
        .field("type", "event_notification")
        .field("body", jsonObject)
        .asString();


    System.out.println(httpResponse.getStatus());

    Unirest.shutdown();
  }
}
