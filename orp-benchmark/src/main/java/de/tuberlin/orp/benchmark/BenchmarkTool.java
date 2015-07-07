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

package de.tuberlin.orp.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.util.concurrent.RateLimiter;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Request;
import com.ning.http.client.Response;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BenchmarkTool {

  private AsyncHttpClient httpClient;
  private BenchmarkConfig config;

  private AtomicInteger requestsCounter = new AtomicInteger(0);
  private AtomicBoolean stopped = new AtomicBoolean(false);

  private RateLimiter rateLimiter;
  private RequestProvider requestProvider;

  private ExecutorService callbackService;


  public BenchmarkTool(BenchmarkConfig config) {
    this.config = config;
    httpClient = new AsyncHttpClient();
    requestProvider = new FileRequestProvider(config.getFilePath(), config);
  }

  private void sendRequest() {
    Request request = requestProvider.getNext();

    rateLimiter.acquire();
    requestsCounter.incrementAndGet();
    ListenableFuture<Response> requestFuture = httpClient.executeRequest(request);
    if (callbackService != null && request.getFormParams().get(0).getValue().equals("recommendation_request")) {
      requestFuture.addListener(() -> {
        try {
          Response response = requestFuture.get();
          System.out.println(response.getResponseBody());
        } catch (InterruptedException | ExecutionException | IOException ignored) {
        }
      }, callbackService);
    }
  }

  private void startRequestCounter() {
    ScheduledExecutorService requestCounterService = Executors.newSingleThreadScheduledExecutor();
    requestCounterService
        .scheduleAtFixedRate(() -> {
          System.out.println("Current throughput = " + requestsCounter.get() + " req/s");
          requestsCounter.set(0);
          if (stopped.get()) {
            requestCounterService.shutdown();
          }
        }, 0, 1000, TimeUnit.MILLISECONDS);
  }

  private void startWarmupPhase(int warmupSteps, long warmupMillis, int maxRate, long duration) {
    AtomicInteger stepsCounter = new AtomicInteger(1);
    long period = warmupMillis / warmupSteps;
    double rateStep = maxRate / (double) warmupSteps;

    rateLimiter = RateLimiter.create(rateStep);

    ScheduledExecutorService warmupService = Executors.newSingleThreadScheduledExecutor();
    warmupService.scheduleAtFixedRate(() -> {
      int cnt = stepsCounter.getAndIncrement();
      if (cnt < warmupSteps) {
        rateLimiter.setRate(rateStep * (cnt + 1));
        if (cnt == warmupSteps - 1) {
          Executors.newSingleThreadScheduledExecutor()
              .schedule((Runnable) () -> {
                stopped.set(true);
                System.out.println("Done sending.");
              }, duration, TimeUnit.MILLISECONDS);
          warmupService.shutdown();
        }
      }
    }, period, period, TimeUnit.MILLISECONDS);

  }


  public void start() {

    startWarmupPhase(config.getWarmupSteps(), config.getWarmup(), config.getRate(), config.getLoadDuration());

    startRequestCounter();

    while (!stopped.get()) {
      sendRequest();
    }

  }

  public static void main(String[] args) throws Exception {

    // G:/json/CLEF-2015-Task2-Json07/Json-07/2014-07-01.data/2014-07-01.data
    // /Users/ilya/Desktop/2014-07-01.data

    BenchmarkConfig config = new BenchmarkConfig();
    JCommander jCommander = new JCommander(config);
    try {
      jCommander.parse(args);
    } catch (ParameterException e) {
      jCommander.usage();
      System.exit(1);
    }

    new BenchmarkTool(config).start();
  }
}
