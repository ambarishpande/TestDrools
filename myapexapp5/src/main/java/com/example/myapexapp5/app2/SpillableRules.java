package com.example.myapexapp5.app2;


import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.persistence.Persistence;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.jbpm.persistence.JpaProcessPersistenceContextManager;
import org.kie.api.KieBase;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.KieServices;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.event.rule.ObjectDeletedEvent;
import org.kie.api.event.rule.ObjectInsertedEvent;
import org.kie.api.event.rule.ObjectUpdatedEvent;
import org.kie.api.event.rule.RuleRuntimeEventListener;
import org.kie.api.marshalling.Marshaller;
import org.kie.api.runtime.Environment;
import org.kie.api.runtime.EnvironmentName;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.KnowledgeBaseFactory;
import org.kie.internal.persistence.jpa.JPAKnowledgeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.rules.Counter;
import com.example.rules.Output;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.common.util.BaseOperator;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.Configuration;
import bitronix.tm.TransactionManagerServices;
import bitronix.tm.resource.jdbc.PoolingDataSource;

public class SpillableRules extends BaseOperator implements Operator.CheckpointNotificationListener,
  ActivationListener<Context.OperatorContext>
{
  transient KieServices kieServices;
  transient KieContainer kieContainer;
  transient KieBase kieBase;
  transient KieSession kieSession;

  private byte[] sessionSnapshot;
  private transient Marshaller marshaller;
  private transient Output outputAdapter;
  private transient Environment env;

  private String storePath = "/user/ambarish/rulesState";

//  ManagedStateSpillableStateStore store;
//  SpillableMapImpl<Long, Measure> measures;
  int sessionId;
  long currentId;
  private long checkpointStartTime;
  private long checkpointEndTime;
  private transient UserTransaction ut;
  @AutoMetric
  public int numberOfFactsInSession;

  private static final Logger logger = LoggerFactory.getLogger(com.example.myapexapp5.app2.SpillableRules.class);

  @Override
  public void setup(Context.OperatorContext context)
  {
    numberOfFactsInSession=0;
    setupDataSource();
    setupTransaction();

//    if (measures == null) {
//      store = new ManagedStateSpillableStateStore();
//      ((FileAccessFSImpl)store.getFileAccess()).setBasePath(storePath);
////      UnboundedTimeBucketAssigner timeBucketAssigner = new UnboundedTimeBucketAssigner();
//      MovingBoundaryTimeBucketAssigner timeBucketAssigner = new MovingBoundaryTimeBucketAssigner();
//      // Time bucket duration 1hr
//      //timeBucketAssigner.setBucketSpan(new Duration(60*60*1000L));
//      timeBucketAssigner.setExpireBefore(new Duration(60*60*1000L));
//      store.setTimeBucketAssigner(timeBucketAssigner);
//      measures = new SpillableMapImpl<>(store, new byte[] {0}, 0L, new LongSerde(), new GenericSerde<Measure>());
//    }
//    store.setup(context);
//    measures.setup(context);

  }

  private void setupTransaction()
  {

    try {
      ut = (UserTransaction) new InitialContext().lookup( "java:comp/UserTransaction" );
      ut.setTransactionTimeout(15000);
      ut.begin();

    } catch (NamingException e) {
      e.printStackTrace();
    } catch (NotSupportedException e) {
      e.printStackTrace();
    } catch (SystemException e) {
      e.printStackTrace();
    }

  }

  private void initKieBase()
  {
    kieServices = KieServices.Factory.get();
    KieBaseConfiguration kieBaseConfiguration = kieServices.newKieBaseConfiguration();
    kieBaseConfiguration.setOption(EventProcessingOption.STREAM);
    kieContainer = kieServices.getKieClasspathContainer();
//    kieBase = kieContainer.newKieBase(kieBaseConfiguration);
  }

  @Override
  public void teardown()
  {
    kieSession.dispose();
//    measures.teardown();
//    store.teardown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
//    try {
//
//      logger.info("Transaction status : " + ut.getStatus());
//    } catch (SystemException e) {
//      e.printStackTrace();
//    }
//    store.beginWindow(windowId);
//    measures.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
//    while(deletedObjects.peek()!=null){
//      long id = ((SpillableMeasure)(deletedObjects.poll()).getOldObject()).id;
//      measures.remove(id);
//      logger.info("Object Deleted from Managed State : " + id);
//    }
//    measures.endWindow();
//    store.endWindow();
    super.endWindow();
  }

  public transient final DefaultInputPort<Measure> input = new DefaultInputPort<Measure>()
  {
    @Override
    public void process(Measure measure)
    {
      long startTime = System.currentTimeMillis();
//      measures.put(currentId, measure);
//      SpillableMeasure rulesMeasure = new SpillableMeasure(currentId++, measures);
      kieSession.insert(measure);

//      kieSession.insert(measure);
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
//    long storeBeforeCheckpointStart = System.currentTimeMillis();
//    store.beforeCheckpoint(l);
    // Take snapshot of the session for checkpointing
//    long start = System.currentTimeMillis();
//    logger.info("Time for store.beforeCheckpoint() " + (start-storeBeforeCheckpointStart));
//    try {
//      ByteArrayOutputStream baos = new ByteArrayOutputStream();
//      marshaller.marshall(baos, kieSession);
//      baos.close();
//      sessionSnapshot = baos.toByteArray();
//    } catch (IOException e) {
//      logger.info("Exception while taking session checkpoint " + e.getMessage());
//      throw new RuntimeException(e);
//    }
//    long end = System.currentTimeMillis();
//    logger.info("Time Taken for marshalling" + (end-start));
//    checkpointStartTime = System.currentTimeMillis();

  }

  @Override
  public void checkpointed(long l)
  {
//    sessionSnapshot = null;
//    store.checkpointed(l);
//    checkpointEndTime = System.currentTimeMillis();
//    logger.info("Time taken for checkpointing: " + (checkpointEndTime-checkpointStartTime) + "\n");
  }

  @Override
  public void committed(long l)
  {
//    store.committed(l);
    try {
      logger.info("Commiting Transaction " + ut.getStatus());
      ut.commit();//commiting old transaction
      ut.setTransactionTimeout(15000);
      ut.begin();// begin new transactions
      logger.info("Begin new Transaction " + ut.getStatus());
    } catch (HeuristicMixedException e) {
      e.printStackTrace();
    } catch (SystemException e) {
      e.printStackTrace();
    } catch (HeuristicRollbackException e) {
      e.printStackTrace();
    } catch (RollbackException e) {
      e.printStackTrace();
    } catch (NotSupportedException e) {
      e.printStackTrace();
    }
  }

  private KieSession restoreSessionFromSnapshot()
  {
    logger.info("Restoring Session from Snapshot");
    KieSession kieSession;

      logger.info("Trying to restore kieSession...");
      try{
        kieSession = JPAKnowledgeService.loadStatefulKnowledgeSession( sessionId, kieContainer.getKieBase(), null, env );
        logger.info("Session restored! Session Id : " + kieSession.getId());
      }catch (Exception e){
        logger.info("No session to load. " + e);
        return null;
      }
//      for (Object object : kieSession.getObjects(new ObjectFilter()
//      {
//        @Override
//        public boolean accept(Object object)
//        {
//          return (object instanceof SpillableMeasure);
//        }
//      })) {
//        ((SpillableMeasure)object).measures = measures;
//        logger.info(((SpillableMeasure)object).id  + " : " +((SpillableMeasure)object).measures.toString());
//      }
    return kieSession;
  }
  @Override
  public void activate(Context.OperatorContext context)
  {
//
//    Configuration conf = TransactionManagerServices.getConfiguration();
//    conf.setDefaultTransactionTimeout(15000);
//    conf.setDebugZeroResourceTransaction(true);
//

    env = KnowledgeBaseFactory.newEnvironment();
    env.set( EnvironmentName.TRANSACTION_MANAGER, TransactionManagerServices.getTransactionManager() );
    env.set( EnvironmentName.ENTITY_MANAGER_FACTORY, Persistence.createEntityManagerFactory("org.jbpm.persistence.jpa") );
//    env.set(EnvironmentName.PERSISTENCE_CONTEXT_MANAGER, new JpaProcessPersistenceContextManager(env));
//    env.set(EnvironmentName.TASK_PERSISTENCE_CONTEXT_MANAGER, new JPATaskPersistenceContextManager(env));

    initKieBase();
//    marshaller = MarshallerFactory.newMarshaller(kieContainer.getKieBase());
//    if (sessionSnapshot == null) {
//      kieSession = JPAKnowledgeService.newStatefulKnowledgeSession( kieContainer.getKieBase(), null, env ); //
//      sessionId = kieSession.getId();
//      kieSession.insert(new Counter());
//    } else {
//      System.out.println("Reusing existing session");
//      kieSession = restoreSessionFromSnapshot();
//      logger.info("Restored Kie Sesstion " + kieSession.toString() + " ID " + kieSession.getId());
//    }


    kieSession = restoreSessionFromSnapshot();

    if (kieSession == null){
      logger.info("Creating new Session...");
      kieSession = JPAKnowledgeService.newStatefulKnowledgeSession( kieContainer.getKieBase(), null, env );
      sessionId = kieSession.getId();
      kieSession.insert(new Counter());
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
  }

  @Override
  public void deactivate()
  {
//    kieSession.dispose();
//    kieSession.destroy();
  }

  public  void setupDataSource(){
//    PoolingDataSource ds = new PoolingDataSource();
//    ds.setUniqueName( "jdbc/BitronixJTADataSource" );
//    ds.setClassName( "org.h2.jdbcx.JdbcDataSource" );
//    ds.setMaxPoolSize( 3 );
//    ds.setAllowLocalTransactions( true );
//    ds.getDriverProperties().put( "user", "sa" );
//    ds.getDriverProperties().put( "password", "sasa" );
//    ds.getDriverProperties().put( "URL", "jdbc:h2:mem:mydb" );
//    ds.init();


    PoolingDataSource postgres = new PoolingDataSource();
    postgres.setUniqueName("jdbc/postgres");
    postgres.setClassName( "bitronix.tm.resource.jdbc.lrc.LrcXADataSource" );
    postgres.setMaxPoolSize( 3 );
    postgres.setApplyTransactionTimeout(true) ;
    postgres.setAllowLocalTransactions( true );
    postgres.getDriverProperties().put("user", "drools");
    postgres.getDriverProperties().put("password", "drools");
    postgres.getDriverProperties().put("url", "jdbc:postgresql://node35.morado.com:5432/kiesession");
//    postgres.getDriverProperties().put("url", "jdbc:postgresql://localhost:5432/kiesession");
    postgres.getDriverProperties().put("driverClassName", "org.postgresql.Driver")  ;
    postgres.init();
//    emf = Persistence.createEntityManagerFactory("org.jbpm.persistence.jpa");
//    Map<String, String> map = new HashMap<String, String>();
//    Properties props = new Properties();
//    props.put(javax.naming.Context.INITIAL_CONTEXT_FACTORY, "bitronix.tm.jndi.BitronixInitialContextFactory");
//    emf = Persistence.createEntityManagerFactory("org.jbpm.persistence.jpa",props);

  }

  private class RuleEventListener implements RuleRuntimeEventListener
  {

    @Override
    public void objectInserted(ObjectInsertedEvent event)
    {
      numberOfFactsInSession++;
    }

    @Override
    public void objectUpdated(ObjectUpdatedEvent event)
    {

    }

    @Override
    public void objectDeleted(ObjectDeletedEvent event)
    {
      logger.info("Event Deleted");
      numberOfFactsInSession--;
    }
  }


  /**
   * Proxy Event . This will be considered as an event by drools.
   */
//  private static class SpillableMeasure implements com.example.rules.Measure, Serializable
//  {
//
//    transient Map<Long, Measure> measures;
//
//    long id;
//
//    private SpillableMeasure()
//    {
//    }
//
//    public SpillableMeasure(long id, Map<Long, Measure> measures)
//    {
//      this.id = id;
//      this.measures = measures;
//    }
//
//    @Override
//    public double getValue()
//    {
////      logger.info("Trying to get obj : " + id); Measure null after restore from checkpoint
//      return measures.get(id).getValue();
//    }

//  }

}
