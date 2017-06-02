package com.example.myapexapp5;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.marshalling.Marshaller;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.internal.marshalling.MarshallerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class Rules extends BaseOperator implements KryoSerializable
{
  transient KieServices kieServices;
  transient KieContainer kieContainer;
  transient KieBase kieBase;
  //transient StatelessKieSession kieSession;
  transient KieSession kieSession;
  //@FieldSerializer.Bind(JavaSerializer.class)
  //Counter counter;

  private static final Logger logger = LoggerFactory.getLogger(Rules.class);

  @Override
  public void setup(Context.OperatorContext context)
  {
    initKieBase();
    if (kieSession == null) {
      //kieSession = kieContainer.newKieSession();
      kieSession = kieBase.newKieSession();
      //kieSession.insert(counter);
      kieSession.insert(new Counter());
    } else {
      System.out.println("Reusing existing session");
    }
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
    logger.debug("Serializing operator");
    try {
      //kryo.writeObject(output, (kieSession != null) ? Boolean.TRUE : Boolean.FALSE);
      output.writeBoolean((kieSession != null) ? true: false);
      if (kieSession != null) {
        Marshaller marshaller = MarshallerFactory.newMarshaller(kieBase);
        //marshaller.marshall(new NonCloseableOutputStream(output.getOutputStream()), kieSession);
        marshaller.marshall(new NonCloseableOutputStream(output), kieSession);
      }
    } catch (IOException e) {
      logger.error("Write error", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void read(Kryo kryo, Input input)
  {
    //Boolean sessionExists = kryo.readObject(input, Boolean.class);
    logger.debug("Deserialize");
    try {
      if (input.readBoolean()) {
      //if (sessionExists) {
        initKieBase();
        Marshaller marshaller = MarshallerFactory.newMarshaller(kieBase);
        //kieSession = marshaller.unmarshall(input.getInputStream());
        kieSession = marshaller.unmarshall(input);
      }
    } catch (IOException | ClassNotFoundException e) {
      logger.error("Error deserializing", e);
      throw new RuntimeException(e);
    }
  }

  private class NonCloseableOutputStream extends FilterOutputStream
  {
    public NonCloseableOutputStream(OutputStream out)
    {
      super(out);
    }

    @Override
    public void close() throws IOException
    {
    }
  }
}
