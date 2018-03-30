/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.example.benchmarks.rocksdb;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hdht.HDHTWriter;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.example.benchmarks.Generator;
import com.datatorrent.example.benchmarks.hdht.HDSOperator;

/**
 * HDHTBenchmarkApplication
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "RocksDBBenchmarkApplication")
public class HDHTBenchmarkApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Generator gen = dag.addOperator("Generator", new Generator());
    gen.setTupleBlast(300);
    gen.setSleepms(5);
    dag.getOperatorMeta("Generator").getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 1);

    RockdbStoreOperator store = dag.addOperator("Store", new RockdbStoreOperator());

    dag.addStream("s1", gen.out, store.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);
  }
}
