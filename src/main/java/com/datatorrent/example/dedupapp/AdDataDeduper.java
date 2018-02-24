
package com.datatorrent.example.dedupapp;

import com.datatorrent.example.ads.AdInfo;
import com.datatorrent.example.operators.DedupOperator;

public class AdDataDeduper extends DedupOperator
{
  @Override
  public byte[] getKey(Object o)
  {
    AdInfo tuple = (AdInfo)o;
    return tuple.getId().getBytes();
  }
}
