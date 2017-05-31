package com.example.myapexapp5;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.marshalling.Marshaller;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.internal.marshalling.MarshallerFactory;

import com.example.rules.Applicant;
import com.example.rules.Counter;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by pramod on 5/26/17.
 */
public class Rules extends BaseOperator implements Operator.CheckpointNotificationListener
{
  transient KieServices kieServices;
  transient KieContainer kieContainer;
  transient KieBase kieBase;
  //transient StatelessKieSession kieSession;
  transient KieSession kieSession;
  //@FieldSerializer.Bind(JavaSerializer.class)
  //Counter counter;
  byte[] saveSession;

  @Override
  public void setup(Context.OperatorContext context)
  {
    initKieBase();
    if (saveSession == null) {
      //kieSession = kieContainer.newKieSession();
      kieSession = kieBase.newKieSession();
      //kieSession.insert(counter);
      Counter counter = new Counter();
      kieSession.insert(counter);
    } else {
      System.out.println("Reusing existing session");
      //kieSession = kieBase.newKieSession();
      //kieSession.insert(counter);
      initKieSession();
    }

    //counter = new Counter();
    //kieSession.insert(counter);
  }

  private void initKieBase()
  {
    kieServices = KieServices.Factory.get();
    kieContainer = kieServices.getKieClasspathContainer();
    //kieSession = kieContainer.newStatelessKieSession();
    kieBase = kieContainer.getKieBase();
  }

  @Override
  public void teardown()
  {
    kieSession.dispose();
  }

  public transient final DefaultInputPort<Applicant> input = new DefaultInputPort<Applicant>()
  {
    @Override
    public void process(Applicant applicant)
    {
      //kieSession.execute(applicant);
      FactHandle handle = kieSession.insert(applicant);
      kieSession.fireAllRules();
      kieSession.delete(handle);
      output.emit(applicant);
    }
  };

  public transient final DefaultOutputPort<Applicant> output = new DefaultOutputPort<>();

  private void initKieSession()
  {
    Marshaller marshaller = MarshallerFactory.newMarshaller(kieBase);
    ByteArrayInputStream bais = new ByteArrayInputStream(saveSession);
    try {
      kieSession = marshaller.unmarshall(bais);
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beforeCheckpoint(long l)
  {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Marshaller marshaller = MarshallerFactory.newMarshaller(kieBase);
      marshaller.marshall(baos, kieSession);
      baos.close();
      saveSession = baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void checkpointed(long l)
  {
    saveSession = null;
  }

  @Override
  public void committed(long l)
  {

  }
}
