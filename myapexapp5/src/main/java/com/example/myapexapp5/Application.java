/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp5;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="DroolsApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    /*
    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
    randomGenerator.setNumTuples(500);
    */

    DataGenerator dataGenerator = dag.addOperator("dataGenerator", DataGenerator.class);
    Rules rules = dag.addOperator("rules", Rules.class);

    ConsoleOperator cons = dag.addOperator("console", new ConsoleOperator());

    //dag.addStream("randomData", randomGenerator.out, cons.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("data", dataGenerator.output, rules.input);
    dag.addStream("decisions", rules.output, cons.input);
  }
}
