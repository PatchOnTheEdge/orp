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

import com.beust.jcommander.Parameter;

public class BenchmarkConfig {
  @Parameter(names = "-rate", description = "Rate with which requests should be sent [request/s].")
  private int rate;

  @Parameter(names = "-warmup", description = "Warmup Duration [ms].")
  private long warmup;

  @Parameter(names = "-steps", description = "Steps")
  private int warmupSteps;

  @Parameter(names = "-file", description = "File with recorded requests.")
  private String filePath;

  @Parameter(names = "-duration", description = "Duration of load phase [ms].")
  private long loadDuration;

  @Parameter(names = "--help", help = true)
  private boolean help;

  @Parameter(names = "-event")
  private String eventUrl;

  @Parameter(names = "-request")
  private String requestUrl;

  @Parameter(names = "-callbacks")
  private boolean callback;


  public int getRate() {
    return rate;
  }

  public long getWarmup() {
    return warmup;
  }

  public int getWarmupSteps() {
    return warmupSteps;
  }

  public String getFilePath() {
    return filePath;
  }

  public long getLoadDuration() {
    return loadDuration;
  }

  public String getEventUrl() {
    return eventUrl;
  }

  public String getRequestUrl() {
    return requestUrl;
  }

  public boolean isCallback() {
    return callback;
  }
}
