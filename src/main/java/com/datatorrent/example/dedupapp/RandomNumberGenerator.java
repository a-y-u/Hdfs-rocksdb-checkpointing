/**
 * Put your copyright and license info here.
 */
package com.datatorrent.example.dedupapp;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import java.util.Random;

import static java.lang.Thread.sleep;

public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  private int numTuples = 100;
  private transient int count = 0;

  public final transient DefaultOutputPort<Tuple> out = new DefaultOutputPort<Tuple>();

  @Override
  public void beginWindow(long windowId)
  {
      count = 0;
  }

  @Override
  public void emitTuples() {

    Tuple tuple = new Tuple();
    System.out.println(" tuple " + tuple.getSite_id() + " " + tuple.getPage_id() + " " + tuple.getClient_id() + " " + tuple.getAmount());
    try {
      sleep(1000/numTuples);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    out.emit(tuple);

  }


  public int getNumTuples()
  {
    return numTuples;
  }


  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }
}





