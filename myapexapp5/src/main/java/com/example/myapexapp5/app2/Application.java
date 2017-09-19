/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp5.app2;

import org.apache.hadoop.conf.Configuration;

import com.example.myapexapp5.ConsoleOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="DroolsApplication2")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    DataGenerator dataGenerator = dag.addOperator("dataGenerator", DataGenerator.class);
    //Rules rules = dag.addOperator("rules", Rules.class);
    SpillableRules rules = dag.addOperator("rules", SpillableRules.class);

    ConsoleOperator cons = dag.addOperator("console", new ConsoleOperator());

    dag.addStream("data", dataGenerator.output, rules.input);
    dag.addStream("decisions", rules.output, cons.input);
  }
}
