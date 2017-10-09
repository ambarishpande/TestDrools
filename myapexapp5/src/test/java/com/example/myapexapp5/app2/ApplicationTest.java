/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp5.app2;

import java.io.IOException;
import java.util.Random;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }


  @Test public void stateFulRuleTest(){

    SpillableRules rules = new SpillableRules();
    rules.activate(null);
    rules.setup(null);
    rules.beginWindow(0);
    Measure measure = new Measure();
    measure.setPayload(new byte[200]);
    long currentTime = System.currentTimeMillis();
    Random random = new Random(System.currentTimeMillis());
    measure.setValue(((currentTime % 2) + 1) * random.nextInt((int)(currentTime % 50 + 1)));
    rules.input.process(measure);
    Measure measure1 = new Measure();
    measure.setPayload(new byte[200]);
    long currentTime1 = System.currentTimeMillis();
    measure.setValue(((currentTime1 % 2) + 1) * random.nextInt((int)(currentTime1 % 50 + 1)));
    rules.input.process(measure1);


  }

  @Test
  public void dataGen(){
    DataGenerator dataGenerator = new DataGenerator();
    dataGenerator.setEmitCount(5);
    dataGenerator.setup(null);
    dataGenerator.beginWindow(0);
    dataGenerator.emitTuples();
    dataGenerator.emitTuples();
    dataGenerator.emitTuples();
    dataGenerator.emitTuples();
    dataGenerator.emitTuples();
    dataGenerator.endWindow();

  }

  @Test
  public void testDataSource(){
    SpillableRules r = new SpillableRules();
    r.setupDataSource();

  }

  @Test
  public void transactionTimeoutTest(){

    UserTransaction ut = null;
    try {
      ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
      ut.setTransactionTimeout(3); // Transaction should timeout after 3 seconds

      ut.begin();

      System.out.println(">>> Executing...");
      Thread.sleep(5000); // Block for 5 seconds

      ut.commit();
      System.out.println(">>> Completed");

    } catch (NamingException e) {
      e.printStackTrace();
    } catch (RollbackException e) {
      e.printStackTrace();
    } catch (NotSupportedException e) {
      e.printStackTrace();
    } catch (SystemException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (HeuristicMixedException e) {
      e.printStackTrace();
    } catch (HeuristicRollbackException e) {
      e.printStackTrace();
    }

  }
}
