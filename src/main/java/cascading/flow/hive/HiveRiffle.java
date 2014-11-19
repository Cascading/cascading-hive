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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import cascading.CascadingException;
import cascading.tap.Tap;
import riffle.process.DependencyIncoming;
import riffle.process.DependencyOutgoing;
import riffle.process.ProcessComplete;
import riffle.process.ProcessStart;
import riffle.process.ProcessStop;

/**
 * Riffle process encapsulating Hive operations.
 */
@riffle.process.Process
class HiveRiffle
  {
  /** List of source taps */
  private final Collection<Tap> sources;

  /** sink of the Riffle. */
  private final Tap sink;

  /** a hive driver factory */
  private final HiveDriverFactory driverFactory;

  /** The hive queries to run */
  private String queries[];

  private Future<Throwable> future;

  /**
   * Constructs a new HiveRiffle with the given HiveConf object, queries, a list of source taps and a sink.
   *
   * @param driverFactory a factory for creating Driver instances.
   * @param queries    The hive queries to run.
   * @param sources The source taps of the queries.
   * @param sink  The sink of the queries.
   */
  @ConstructorProperties({"hiveConf", "queries", "sources", "sink"})
  HiveRiffle( HiveDriverFactory driverFactory, String queries[], Collection<Tap> sources, Tap sink )
    {
    this.driverFactory = driverFactory;
    this.queries = queries;
    if ( sources == null || sources.isEmpty() )
      throw new CascadingException( "sources cannot be null or empty" );
    this.sources = sources;
    this.sink = sink;
    }

  @ProcessStart
  public void start()
    {
    internalStart();
    }

  private synchronized void internalStart()
    {
    if ( future != null )
      return;
    ExecutorService executorService = Executors.newSingleThreadExecutor( new HiveFlowThreadFactory() );
    Callable<Throwable> queryRunner =  new HiveQueryRunner( driverFactory, queries );
    future = executorService.submit( queryRunner );
    executorService.shutdown();
    }

  /**
   * stop method. currently does nothing, but is required by riffle.
   */
  @ProcessStop
  public void stop()
    {
    // since it is unclear in which state the system is left, if we interrupt the running thread, we opt for a noop for now.
    }

  @ProcessComplete
  public void complete()
    {
    internalStart();
    try
      {
      Throwable throwable = future.get();
      if( throwable != null )
        {
        if( throwable instanceof RuntimeException )
          throw ( (RuntimeException) throwable );
        else
          throw new CascadingException( "exception while executing hive queries", throwable );
        }
      }
      catch( Exception exception )
        {
        throw new CascadingException( "exception while executing hive queries", exception );
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
    return sources ;
    }


  class HiveFlowThreadFactory implements ThreadFactory
    {
    @Override
    public Thread newThread( Runnable r )
      {
      return new Thread( r, "hive-flow" );
      }
    }

  }
