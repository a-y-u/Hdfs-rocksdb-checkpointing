package com.datatorrent.example.ads;

import java.util.ArrayList;
import java.util.Random;

import org.slf4j.Logger;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import static org.slf4j.LoggerFactory.getLogger;

public class AdsGeneratorOperator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = getLogger(AdsGeneratorOperator.class);

  public transient final DefaultOutputPort<AdInfo> outputPort = new DefaultOutputPort<>();

  // Configuration
  private int tuplesPerSecond = 10;
  private double duplicateProbability = 0.01d;

  // Metrics
  @AutoMetric
  public long numDuplicates = 0;
  @AutoMetric
  public long numDuplicateCandidates = 0;

  private int count = 0;
  private transient AdInfoGenerator generator = null;
  private transient Random rand = new Random();
  private transient LimitedSizeQueue<AdInfo> duplicateCandidates;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    try {
      generator = new AdInfoGenerator();
      duplicateCandidates = new LimitedSizeQueue<>(1024);
    } catch (Exception ex) {
      LOG.error("Unable to initialize adInfo Generator {}", ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if (count > tuplesPerSecond) {
      return;
    }

    AdInfo ad = generator.generateRecord();
    if (duplicateProbability > 0 && (rand.nextDouble() < duplicateProbability) && duplicateCandidates.size() > 0) {
      int idx = rand.nextInt(duplicateCandidates.size());
      numDuplicates++;
      ad = duplicateCandidates.get(idx);
    }
    // add elements to duplicate list for generating duplicates.
    if (rand.nextDouble() < 0.001) {
      duplicateCandidates.add(ad);
      numDuplicateCandidates = duplicateCandidates.size();
    }
    outputPort.emit(ad);
    count++;
  }


  public static class LimitedSizeQueue<K> extends ArrayList<K>
  {

    private int maxSize;

    public LimitedSizeQueue(int size)
    {
      this.maxSize = size;
    }

    public boolean add(K k)
    {
      boolean r = super.add(k);
      if (size() > maxSize) {
        removeRange(0, size() - maxSize - 1);
      }
      return r;
    }

    public K getYongest()
    {
      return get(size() - 1);
    }

    public K getOldest()
    {
      return get(0);
    }
  }
}
