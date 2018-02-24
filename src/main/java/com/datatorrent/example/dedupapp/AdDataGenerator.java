/**
 * Put your copyright and license info here.
 */
package com.datatorrent.example.dedupapp;

import org.slf4j.Logger;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import static java.lang.Thread.sleep;
import static org.slf4j.LoggerFactory.getLogger;

public class AdDataGenerator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = getLogger(AdDataGenerator.class);
  private int numTuples = 100;
  private transient int count = 0;

  public final transient DefaultOutputPort<AdData> out = new DefaultOutputPort<AdData>();

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    AdData tuple = new AdData();
    LOG.trace("AdData {} {} {} {}", tuple.getSite_id(), tuple.getPage_id(), tuple.getClient_id(), tuple.getAmount());
    try {
      sleep(1000 / numTuples);
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





