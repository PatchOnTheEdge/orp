package de.tuberlin.orp.common;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by Patch on 21.08.2015.
 */

/**
 * An ArrayDeque with a maximum Size.
 * Tail represents the newest added item.
 * Head represents the oldest item.
 */
public class LiFoRingBuffer{
  private int maxSize;
  private ArrayDeque<Object> buffer;


  public LiFoRingBuffer(int maxSize) {
    if (maxSize<1){
      throw new RuntimeException("Maximum size must be at least 1");
    }
    this.maxSize = maxSize;
    this.buffer = new ArrayDeque<>(maxSize);
  }
  public void add(Object context){
    if (buffer.size() == maxSize){
      buffer.removeFirst();
    }
    buffer.add(context);
  }

  public ArrayDeque<Object> getBuffer() {
    return buffer;
  }
}

