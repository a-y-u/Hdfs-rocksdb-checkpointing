
package com.datatorrent.example.dedupapp;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.example.ads.AdsGeneratorOperator;
import com.datatorrent.example.operators.JsonConverter;

@ApplicationAnnotation(name = "MyFirstApplication")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    AdsGeneratorOperator randomGenerator = dag.addOperator("AdDataGenerator", AdsGeneratorOperator.class);

    AdDataDeduper dedup = dag.addOperator("Dedup", new AdDataDeduper());

    JsonConverter json = dag.addOperator("JSONConverter", new JsonConverter());

    GenericFileOutputOperator.StringFileOutputOperator outputOperator = dag.addOperator("DuplicatesWriter", new GenericFileOutputOperator.StringFileOutputOperator());
    outputOperator.setFilePath("/tmp/duplicates");
    outputOperator.setOutputFileName("duplicates");
    outputOperator.setMaxLength(128 * 1024 * 1024);

    dag.addStream("randomData", randomGenerator.outputPort, dedup.inputPort);
    dag.addStream("duplicates", dedup.duplicateData, json.input);
    dag.addStream("dup_json", json.output, outputOperator.input);
  }
}
