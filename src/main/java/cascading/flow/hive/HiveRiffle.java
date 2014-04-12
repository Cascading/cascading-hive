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

import cascading.CascadingException;
import cascading.tap.Tap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import riffle.process.DependencyIncoming;
import riffle.process.DependencyOutgoing;
import riffle.process.ProcessComplete;
import riffle.process.ProcessStart;
import riffle.process.ProcessStop;

/**
 * Riffle process encapsulating Hive operations.
 */
@riffle.process.Process
public class HiveRiffle
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HiveRiffle.class );

  /** List of source taps */
  private final Collection<Tap> sources;

  /** sink of the Riffle. */
  private final Tap sink;

  /** a hive driver factory */
  private final HiveDriverFactory driverFactory;

  /** The hive query to run */
  private String query;

  /**
   * Constructs a new HiveRiffle with the given HiveConf object, query, a list of source taps and a sink.
   *
   * @param driverFactory a factory for creating Driver instances.
   * @param query    The hive query to run.
   * @param sources The source taps of the query.
   * @param sink  The sink of the query.
   */
  @ConstructorProperties({"hiveConf", "query", "sources", "sink"})
  HiveRiffle( HiveDriverFactory driverFactory, String query, Collection<Tap> sources, Tap sink )
    {
    this.driverFactory = driverFactory;
    this.query = query;
    if ( sources == null || sources.isEmpty() )
      throw new CascadingException( "sources cannot be null or empty" );
    this.sources = sources;
    this.sink = sink;
    }

  /**
   * start method. currently does nothing, but is required by riffle.
   */
  @ProcessStart
  public void start()
    {
    //NOOP
    }

  /**
   * stop method. currently does nothing, but is required by riffle.
   */
  @ProcessStop
  public void stop()
    {
    // NOOP
    }

  @ProcessComplete
  public void complete()
    {
    HiveQueryRunner runner = new HiveQueryRunner( driverFactory, query );
    runner.run();
    }

  @DependencyOutgoing
  public Collection getOutgoing()
    {
    return Collections.unmodifiableCollection( Arrays.asList( sink ) );
    }

  @DependencyIncoming
  public Collection getIncoming()
    {
    return sources ;
    }

  }
