package com.example.myapexapp5.app2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.kie.api.KieBase;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.KieServices;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.marshalling.Marshaller;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.marshalling.MarshallerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.mutable.MutableInt;

import com.example.rules.Output;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

public class Rules extends BaseOperator implements Operator.CheckpointNotificationListener
{
  transient KieServices kieServices;
  transient KieContainer kieContainer;
  transient KieBase kieBase;
  transient KieSession kieSession;

  private byte[] sessionSnapshot;
  private transient Marshaller marshaller;
  private transient Output outputAdapter;

  private static final Logger logger = LoggerFactory.getLogger(com.example.myapexapp5.Rules.class);

  @Override
  public void setup(Context.OperatorContext context)
  {
    initKieBase();
    //marshaller = MarshallerFactory.newMarshaller(kieContainer.getKieBase());
    marshaller = MarshallerFactory.newMarshaller(kieBase);
    if (sessionSnapshot == null) {
      kieSession = kieBase.newKieSession();
    } else {
      System.out.println("Reusing existing session");
      restoreSessionFromSnapshot();
    }
    outputAdapter = new Output() {

      @Override
      public void emit(String s)
      {
        output.emit(s);
      }
    };
    kieSession.setGlobal("output", outputAdapter);
    kieSession.setGlobal("counter", new MutableInt());
  }

  private void initKieBase()
  {
    kieServices = KieServices.Factory.get();
    KieBaseConfiguration kieBaseConfiguration = kieServices.newKieBaseConfiguration();
    kieBaseConfiguration.setOption(EventProcessingOption.STREAM);
    kieContainer = kieServices.getKieClasspathContainer();
    kieBase = kieContainer.newKieBase(kieBaseConfiguration);
  }

  @Override
  public void teardown()
  {
    kieSession.dispose();
  }

  public transient final DefaultInputPort<Measure> input = new DefaultInputPort<Measure>()
  {
    @Override
    public void process(Measure measure)
    {
      long startTime = System.currentTimeMillis();
      kieSession.insert(measure);
      kieSession.fireAllRules();
      if ((System.currentTimeMillis() - startTime) >= 1000) {
        logger.info("Processing time {}", System.currentTimeMillis() - startTime);
      }
    }
  };

  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<>();

  @Override
  public void beforeCheckpoint(long l)
  {
    // Take snapshot of the session for checkpointing
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      marshaller.marshall(baos, kieSession);
      baos.close();
      sessionSnapshot = baos.toByteArray();
    } catch (IOException e) {
      logger.info("Exception while taking session checkpoint " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void checkpointed(long l)
  {
    sessionSnapshot = null;
  }

  @Override
  public void committed(long l)
  {
  }

  private void restoreSessionFromSnapshot()
  {
    ByteArrayInputStream bais = new ByteArrayInputStream(sessionSnapshot);
    try {
      kieSession = marshaller.unmarshall(bais);
    } catch (IOException | ClassNotFoundException e) {
      logger.info("Exception while restoring from session checkpoint " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
