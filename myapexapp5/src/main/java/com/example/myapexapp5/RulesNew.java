package com.example.myapexapp5;

import java.io.IOException;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.marshalling.Marshaller;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.internal.marshalling.MarshallerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.example.rules.Applicant;
import com.example.rules.Counter;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by pramod on 5/26/17.
 */
public class RulesNew extends BaseOperator implements KryoSerializable
{
  transient KieServices kieServices;
  transient KieContainer kieContainer;
  transient KieBase kieBase;
  //transient StatelessKieSession kieSession;
  transient KieSession kieSession;
  //@FieldSerializer.Bind(JavaSerializer.class)
  //Counter counter;

  @Override
  public void setup(Context.OperatorContext context)
  {
    initKieBase();
    if (kieSession == null) {
      //kieSession = kieContainer.newKieSession();
      kieSession = kieBase.newKieSession();
      //kieSession.insert(counter);
      Counter counter = new Counter();
      kieSession.insert(counter);
    } else {
      System.out.println("Reusing existing session");
      //kieSession = kieBase.newKieSession();
      //kieSession.insert(counter);
      //initKieSession();
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

  @Override
  public void write(Kryo kryo, Output output)
  {
    try {
      kryo.writeObject(output, (kieSession != null) ? Boolean.TRUE : Boolean.FALSE);
      if (kieSession != null) {
        Marshaller marshaller = MarshallerFactory.newMarshaller(kieBase);
        marshaller.marshall(output.getOutputStream(), kieSession);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void read(Kryo kryo, Input input)
  {
    Boolean sessionExists = kryo.readObject(input, Boolean.class);
    if (sessionExists) {
      initKieBase();
      Marshaller marshaller = MarshallerFactory.newMarshaller(kieBase);
      try {
        kieSession = marshaller.unmarshall(input.getInputStream());
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
