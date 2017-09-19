package com.example.myapexapp5.app2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.joda.time.Duration;
import org.kie.api.KieBase;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.KieServices;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.event.rule.ObjectDeletedEvent;
import org.kie.api.event.rule.ObjectInsertedEvent;
import org.kie.api.event.rule.ObjectUpdatedEvent;
import org.kie.api.event.rule.RuleRuntimeEventListener;
import org.kie.api.marshalling.Marshaller;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.ObjectFilter;
import org.kie.internal.marshalling.MarshallerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.state.managed.UnboundedTimeBucketAssigner;
import org.apache.apex.malhar.lib.state.spillable.SpillableMapImpl;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.GenericSerde;
import org.apache.apex.malhar.lib.utils.serde.LongSerde;
import org.apache.commons.lang.mutable.MutableInt;

import com.example.rules.Output;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;

public class SpillableRules extends BaseOperator implements Operator.CheckpointNotificationListener
{
  transient KieServices kieServices;
  transient KieContainer kieContainer;
  transient KieBase kieBase;
  transient KieSession kieSession;

  private byte[] sessionSnapshot;
  private transient Marshaller marshaller;
  private transient Output outputAdapter;

  private String storePath = "/user/pramod/rulesState";

  ManagedStateSpillableStateStore store;
  SpillableMapImpl<Long, Measure> measures;

  long currentId;

  private static final Logger logger = LoggerFactory.getLogger(com.example.myapexapp5.Rules.class);

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (measures == null) {
      store = new ManagedStateSpillableStateStore();
      ((FileAccessFSImpl)store.getFileAccess()).setBasePath(storePath);
      UnboundedTimeBucketAssigner timeBucketAssigner = new UnboundedTimeBucketAssigner();
      // Time bucket duration 1hr
      timeBucketAssigner.setBucketSpan(new Duration(60*60*1000L));
      store.setTimeBucketAssigner(timeBucketAssigner);
      measures = new SpillableMapImpl<>(store, new byte[] {0}, 0L, new LongSerde(), new GenericSerde<Measure>());
    }
    store.setup(context);
    measures.setup(context);
    initKieBase();
    //marshaller = MarshallerFactory.newMarshaller(kieContainer.getKieBase());
    marshaller = MarshallerFactory.newMarshaller(kieBase);
    if (sessionSnapshot == null) {
      kieSession = kieBase.newKieSession();
    } else {
      System.out.println("Reusing existing session");
      kieSession = restoreSessionFromSnapshot();
    }
    kieSession.addEventListener(new RuleEventListener());
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
    measures.teardown();
    store.teardown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    store.beginWindow(windowId);
    measures.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    measures.endWindow();
    store.endWindow();
    super.endWindow();
  }

  public transient final DefaultInputPort<Measure> input = new DefaultInputPort<Measure>()
  {
    @Override
    public void process(Measure measure)
    {
      long startTime = System.currentTimeMillis();
      measures.put(currentId, measure);
      SpillableMeasure rulesMeasure = new SpillableMeasure(currentId++, measures);
      kieSession.insert(rulesMeasure);
      kieSession.fireAllRules();
      if ((System.currentTimeMillis() - startTime) >= 1000) {
        logger.info("Processing time {} for {}", System.currentTimeMillis() - startTime, measure.getValue());
      }
    }
  };

  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<>();

  @Override
  public void beforeCheckpoint(long l)
  {
    store.beforeCheckpoint(l);
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
    store.checkpointed(l);
  }

  @Override
  public void committed(long l)
  {
    store.committed(l);
  }

  private KieSession restoreSessionFromSnapshot()
  {
    KieSession kieSession;
    ByteArrayInputStream bais = new ByteArrayInputStream(sessionSnapshot);
    try {
      kieSession = marshaller.unmarshall(bais);
      for ( Object object : kieSession.getObjects(new ObjectFilter()
      {
        @Override
        public boolean accept(Object object)
        {
          return (object instanceof SpillableMeasure);
        }
      })) {
        ((SpillableMeasure)object).measures = measures;
      }
    } catch (IOException | ClassNotFoundException e) {
      logger.info("Exception while restoring from session checkpoint " + e.getMessage());
      throw new RuntimeException(e);
    }
    return kieSession;
  }

  private class RuleEventListener implements RuleRuntimeEventListener
  {

    @Override
    public void objectInserted(ObjectInsertedEvent event)
    {

    }

    @Override
    public void objectUpdated(ObjectUpdatedEvent event)
    {

    }

    @Override
    public void objectDeleted(ObjectDeletedEvent event)
    {
      measures.remove(((SpillableMeasure)event.getOldObject()).id);
    }
  }

  private static class SpillableMeasure implements com.example.rules.Measure, Serializable
  {

    transient Map<Long, Measure> measures;

    long id;

    public SpillableMeasure()
    {
    }

    public SpillableMeasure(long id, Map<Long, Measure> measures)
    {
      this.id = id;
      this.measures = measures;
    }

    @Override
    public double getValue()
    {
      return measures.get(id).getValue();
    }

  }
}
