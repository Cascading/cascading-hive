/*
* Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
*
* Project and contact information: http://www.cascading.org/
*
* This file is part of the Cascading project.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package cascading.flow.hive;

import java.beans.ConstructorProperties;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.hive.util.FieldsUtils;
import cascading.flow.hive.util.TapUtils;
import cascading.scheme.NullScheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flow implementation for running Hive queries within a Cascade.
 */
public class HiveFlow extends HadoopFlow
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HiveFlow.class );

  /** The hive query to run */
  private String query;

  /** HiveConf object */
  private HiveConf hiveConf;

  /**
   * Constructs a new HiveFlow with the given name, properties and query.
   *
   * @param name       The name of the flow object.
   * @param properties Properties, which are important for processing.
   * @param query      The hive query to run.
   */
  @ConstructorProperties({"name", "properties", "query"})
  public HiveFlow( String name, Map<Object, Object> properties, String query )
    {
    this( name, properties, query, null );
    }

  /**
   * Constructs a new HiveFlow with the given name, properties, query and a custom HiveConf object. This constructor
   * is currently meant for testing only.
   *
   * @param name       The name of the flow object.
   * @param properties Properties, which are important for processing.
   * @param query      The hive query to run.
   * @param hiveConf
   */
  @ConstructorProperties({"name", "properties", "query", "hiveConf"})
  HiveFlow( String name, Map<Object, Object> properties, String query, HiveConf hiveConf )
    {
    setName( "hive: " + name );
    this.hiveConf = hiveConf;
    this.query = query;
    initFromProperties( properties );
    initConfig( properties, new JobConf() );
    try
      {
      initialize();
      }
    // Hive throws java.lang.Exception on some operations, so we have to catch and properly wrap that here.
    catch( Exception exception )
      {
      throw new CascadingException( "problem in hive flow preparation", exception );
      }
    }

  @Override
  public void start()
    {
    }

  @Override
  public void stop()
    {
    fireOnStopping();
    if( !flowStats.isFinished() )
      flowStats.markStopped();
    }

  @Override
  public void complete()
    {
    boolean failed = false;
    Driver driver = null;
    try
      {
      fireOnStarting();
      flowStats.markStarted();
      driver = createHiveDriver();
      flowStats.markRunning();
      LOG.info( "running query {}", query );
      CommandProcessorResponse response = driver.run( this.query );
      if( response.getResponseCode() != 0 )
        {
        CascadingException exception = new CascadingException( "hive error '" + response.getErrorMessage() + "' while running query " + query );
        flowStats.markFailed( exception );
        failed = true;
        throw exception;
        }
      flowStats.markSuccessful();
      }
    catch( CommandNeedRetryException exception )
      {
      flowStats.markFailed( exception );
      throw new CascadingException( "problem while executing hive query: " + query, exception );
      }
    finally
      {
      if( !failed )
        fireOnCompleted();
      flowStats.cleanup();
      if( driver != null )
        driver.destroy();
      }
    }


  /**
   * Initializes the HiveFlow instance by compiling the supplied query and reading data from various sources so that
   * cascading can work with it.
   *
   * @throws Exception in case any interaction with hive goes wrong.
   */
  private void initialize() throws Exception
    {
    Driver driver = null;
    try
      {
      driver = createHiveDriver();
      int result = driver.compile( query );
      if( result != 0 )
        throw new CascadingException( "unable to compile query " + query );

      // the plan and the schema contain what we need to build proper taps/schemes/fields
      Schema schema = driver.getSchema();
      QueryPlan plan = driver.getPlan();

      Map<String, Tap> sinks = new HashMap<String, Tap>();
      Map<String, Tap> sources = new HashMap<String, Tap>();

      // in order to give Cascading proper Taps/Schemes/Fields we have to either look into inputs and outputs
      // of the plan or look at the tasks, that hive is going to run
      createTapsFromTask( sinks, sources, plan.getRootTasks().get( 0 ) );

      Iterator<WriteEntity> outputIterator = plan.getOutputs().iterator();
      if( outputIterator.hasNext() )
        {
        // clear, since the output iterator has the actual files on HDFS, if it is not empty in the
        // 'load data into' case.
        sinks.clear();

        WriteEntity outputEntity = outputIterator.next();
        String path = outputEntity.getLocation().toString();
        Fields sinkFields = FieldsUtils.fromFieldSchemaList( schema.getFieldSchemas() );

        Tap sink = TapUtils.createTap( path, sinkFields, SinkMode.REPLACE );
        sinks.put( sink.getIdentifier(), sink );
        }

      Iterator<ReadEntity> inputIterator = plan.getInputs().iterator();
      while( inputIterator.hasNext() )
        {
        // TODO figure out what parents are and if we need them
        ReadEntity readEntity = inputIterator.next();
        String inputPath = readEntity.getLocation().toString();
        Tap source = TapUtils.createTap( inputPath, Fields.UNKNOWN, SinkMode.REPLACE );
        sources.put( inputPath, source );
        }

      if( sinks.isEmpty() )
        throw new CascadingException( "unable to determine sink from hive query " + query );
      setSinks( sinks );
      if( sources.isEmpty() )
        throw new CascadingException( "unable to determine sources from hive query " + query );
      setSources( sources );

      setTraps( new HashMap<String, Tap>() );
      }
    finally
      {
      if( driver != null )
        driver.destroy();
      }
    }

  /**
   * Internal method that tries to determine the sources and sinks by analyzing the supplied hive Task.
   *
   * @param sinks   The sink map.
   * @param sources The sources map.
   * @param task    The hive task, which might have the information we want.
   */
  private void createTapsFromTask( Map<String, Tap> sinks, Map<String, Tap> sources, Task task )
    {
    if( task.getType() == StageType.DDL )
      {
      DDLWork ddlWork = (DDLWork) task.getWork();
      if( ddlWork.getCreateTblDesc() != null )
        {
        String path = ddlWork.getCreateTblDesc().getLocation();
        if( path == null )
          path = String.format( "%s/%s",
            hiveConf.get( HiveConf.ConfVars.METASTOREWAREHOUSE.varname ), ddlWork.getCreateTblDesc().getTableName() );

        Fields sinkFields = FieldsUtils.fromFieldSchemaList( ddlWork.getCreateTblDesc().getCols() );

        Tap sink = TapUtils.createTap( path, sinkFields, SinkMode.REPLACE, ddlWork.getCreateTblDesc().getFieldDelim() );
        sinks.put( sink.getIdentifier(), sink );

        sources.put( HiveNullTap.DEV_ZERO.getIdentifier(), HiveNullTap.DEV_ZERO );
        }
      //TODO figure this out
      /*else if (ddlWork.getCreateTblLikeDesc() != null)
        {
        String path = ddlWork.getCreateTblLikeDesc().getLocation();
        if ( path == null )
          path = String.format( "%s/%s",
            hiveConf.get( HiveConf.ConfVars.METASTOREWAREHOUSE.varname ), ddlWork.getCreateTblDesc().getTableName() );

        sinks.put( path, new Hfs( new NullScheme(), path, SinkMode.REPLACE ) );
        sources.put( HiveNullTap.DEV_ZERO.getIdentifier(), HiveNullTap.DEV_ZERO );
        } */
      }
    else if( task.getType() == StageType.COPY )
      {
      CopyWork cw = (CopyWork) task.getWork();
      String sinkPath = cw.getToPath();
      sinks.put( sinkPath, new Hfs( new NullScheme(), sinkPath, SinkMode.REPLACE ) );
      String sourcePath = cw.getFromPath();
      sources.put( sourcePath, new Hfs( new NullScheme(), sourcePath ) );
      }
    }

  /**
   * Creates a new Driver instance and sets everything up for compiling/processing queries
   *
   * @return a new Driver instance.
   */
  private Driver createHiveDriver()
    {
    if( hiveConf == null )
      hiveConf = createHiveConf();
    // TODO do we need a global lock here? This looks problematic with concurrent flows running.
    SessionState.start( hiveConf );
    SessionState.get().setIsSilent( true );
    Driver driver = new Driver( hiveConf );
    driver.init();
    return driver;
    }

  /**
   * Helper method to create a {HiveConf} object.
   *
   * @return a fully initialized HiveConf object.
   */
  private HiveConf createHiveConf()
    {
    HiveConf hiveConf = new HiveConf();
    JobConf jobConf = getConfigCopy();

    String metaStoreUris = jobConf.get( HiveProps.METASTORE_URIS );
    if( metaStoreUris == null )
      LOG.info( "using local hive MetaStore" );
    else
      {
      LOG.info( "using hive MetaStore at {} ", metaStoreUris );
      hiveConf.set( HiveProps.METASTORE_URIS, jobConf.get( HiveProps.METASTORE_URIS ) );
      }

    // hive submits the jobs itself, so we have to tell it the jobtracker and hdfs master
    hiveConf.set( "mapred.job.tracker", jobConf.get( "mapred.job.tracker" ) );
    hiveConf.set( "fs.default.name", jobConf.get( "fs.default.name" ) );

    //HiveConf.setBoolVar( hiveConf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false );

    hiveConf.set( HiveConf.ConfVars.HIVESESSIONSILENT.varname, "true" );

    return hiveConf;
    }

  @Override
  public boolean stepsAreLocal()
    {
    return true;
    }

  @Override
  public String getTags()
    {
    return "hive";
    }

  }
