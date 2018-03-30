
package com.datatorrent.example.dedupapp;

import com.datatorrent.example.operators.AggregationOperator;
import com.datatorrent.example.operators.JsonConverterUnique;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.example.ads.AdsGeneratorOperator;
import com.datatorrent.example.operators.JsonConverterDuplicate;

@ApplicationAnnotation(name = "DedupApplication")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    AdsGeneratorOperator randomGenerator = dag.addOperator("AdDataGenerator", AdsGeneratorOperator.class);

    AdDataDeduper dedup = dag.addOperator("Dedup", new AdDataDeduper());

    JsonConverterDuplicate json = dag.addOperator("JSONConverter", new JsonConverterDuplicate());

    GenericFileOutputOperator.StringFileOutputOperator outputOperator = dag.addOperator("DuplicatesWriter", new GenericFileOutputOperator.StringFileOutputOperator());
    outputOperator.setFilePath("/tmp/duplicates");
    outputOperator.setOutputFileName("duplicates");
    outputOperator.setMaxLength(128 * 1024 * 1024);


    JsonConverterUnique jsonUnique = dag.addOperator("JSONConverter1", new JsonConverterUnique());

    GenericFileOutputOperator.StringFileOutputOperator outputOperatorTuple = dag.addOperator("UniquesWriter", new GenericFileOutputOperator.StringFileOutputOperator());
    outputOperatorTuple.setFilePath("/tmp/uniques");
    outputOperatorTuple.setOutputFileName("uniques");
    outputOperatorTuple.setMaxLength(128 * 1024 * 1024);

    AggregationOperator unique = dag.addOperator("UniqueAD", new AggregationOperator());

    dag.addStream("randomData", randomGenerator.outputPort, dedup.inputPort);
    dag.addStream("duplicates", dedup.duplicateData, json.input);
    dag.addStream("uniques",unique.outputData,jsonUnique.input);
    dag.addStream("unique",dedup.uniqueData,unique.input);
    dag.addStream("dup_json", json.output, outputOperator.input);
    dag.addStream("unique_json", jsonUnique.output, outputOperatorTuple.input);
  }
}
