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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import cascading.CascadingException;
import cascading.scheme.NullScheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import riffle.process.DependencyIncoming;
import riffle.process.DependencyOutgoing;
import riffle.process.ProcessComplete;
import riffle.process.ProcessStart;
import riffle.process.ProcessStop;

/**
 * Flow implementation for running Hive queries within a Cascade.
 */
@riffle.process.Process
public class HiveFlow
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HiveFlow.class );
  private final List<Tap> sources;
  private final Tap sink;

  /** The hive query to run */
  private String query;

  /** HiveConf object */
  private HiveConf hiveConf;

  /**
   * Constructs a new HiveFlow with the given name, properties, query and a custom HiveConf object. This constructor
   * is currently meant for testing only.
   *
   * @param hiveConf The HiveConf object to use
   * @param query    The hive query to run.
   */
  @ConstructorProperties({"hiveConf"})
  public HiveFlow( HiveConf hiveConf, String query, List<Tap> sources, Tap sink )
    {
    this.hiveConf = hiveConf;
    this.query = query;
    this.sources = sources;
    this.sink = sink;
    }


  @ProcessStart
  public void start()
    {
    //NOOP, but required for riffle
    }

  @ProcessStop
  public void stop()
    {
    // NOOP, but required for riffle
    }

  @ProcessComplete
  public void complete()
    {
    Driver driver = null;
    try
      {
      driver = createHiveDriver();
      LOG.info( "running query {}", query );
      CommandProcessorResponse response = driver.run( this.query );
      if( response.getResponseCode() != 0 )
        {
        throw new CascadingException( "hive error '" + response.getErrorMessage() + "' while running query " + query );
        }
      }
    catch( CommandNeedRetryException exception )
      {
      throw new CascadingException( "problem while executing hive query: " + query, exception );
      }
    finally
      {
      if( driver != null )
        driver.destroy();
      }
    }

  @DependencyOutgoing
  public Collection getOutgoing()
    {
    return Collections.unmodifiableCollection( Arrays.asList( sink ) );
    }

  @DependencyIncoming
  public Collection getIncoming()
    {
    return Collections.unmodifiableCollection( sources );
    }

  /**
   * Creates a new Driver instance and sets everything up for compiling/processing queries
   *
   * @return a new Driver instance.
   */
  private Driver createHiveDriver()
    {
    // TODO do we need a global lock here? This looks problematic with concurrent flows running.
    SessionState.start( hiveConf );
    SessionState.get().setIsSilent( true );
    Driver driver = new Driver( hiveConf );
    driver.init();
    return driver;
    }

  }
