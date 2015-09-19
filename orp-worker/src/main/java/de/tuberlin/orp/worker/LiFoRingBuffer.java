package de.tuberlin.orp.worker;

import de.tuberlin.orp.common.message.OrpContext;

import java.lang.reflect.Array;
import java.util.ArrayList;

/**
 * Created by Patch on 21.08.2015.
 */
public class LiFoRingBuffer {
  private int bufferSize;
  private OrpContext[] buffer;
  private int lastElement;
  private int nrOfElements;

  public LiFoRingBuffer(int bufferSize) {
    this.bufferSize = bufferSize;
    this.buffer = new OrpContext[bufferSize];
    this.lastElement = -1;
    this.nrOfElements = 0;
  }
  public void add(OrpContext context){
    if (isEmpty()){
      buffer[0] = context;
      lastElement = 0;
      nrOfElements = 1;
    }
    else if((lastElement == bufferSize - 1)){
      buffer[0] = context;
      lastElement = 0;
      if (nrOfElements<bufferSize){
        nrOfElements++;
      }
    }
    else{
      buffer[lastElement + 1] = context;
      lastElement++;
      nrOfElements++;
    }
  }
  public OrpContext pop(){
    if (isEmpty()){
      throw new RuntimeException("Buffer is Empty!");
    }
    else{
      lastElement--;
      nrOfElements--;
      return buffer[lastElement + 1];
    }
  }
  public OrpContext getLast(){
    if (isEmpty()){
      throw new RuntimeException("Buffer is Empty!");
    }
    else {
      return buffer[lastElement];
    }
  }
  public OrpContext[] getLastN(int n){
    if (nrOfElements < n){
      throw new RuntimeException("To few Elements");
    }
    else {
      OrpContext[] lastN = new OrpContext[n];
      for (int i = 0; i < n; i++) {
        if (lastElement - i >= 0){
          lastN[i] = buffer[lastElement-i];
        }
        else{
          lastN[i] = buffer[lastElement+bufferSize-i];
        }
      }
      return lastN;
    }
  }
  public boolean isEmpty(){
    return lastElement < 0;
  }
}

