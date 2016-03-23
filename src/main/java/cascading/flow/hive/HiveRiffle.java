/*
* Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import cascading.CascadingException;
import cascading.tap.Tap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import riffle.process.DependencyIncoming;
import riffle.process.DependencyOutgoing;
import riffle.process.ProcessChildren;
import riffle.process.ProcessComplete;
import riffle.process.ProcessConfiguration;
import riffle.process.ProcessCounters;
import riffle.process.ProcessStart;
import riffle.process.ProcessStop;

/**
 * Riffle process encapsulating Hive operations.
 */
@riffle.process.Process
class HiveRiffle implements CascadingHiveHistoryDecorator
  {
  /** List of source taps */
  private final Collection<Tap> sources;

  /** sink of the Riffle. */
  private final Tap sink;

  /** a hive driver factory */
  private final HiveDriverFactory driverFactory;

  /** The hive queries to run */
  private String queries[];

  /** A future running the current queries asynchronously. */
  private Future<Throwable> future;

  /** The internal HiveHistory instance of the current SessionState. */
  private HiveHistory original;

  /** set to keep track of all counters of all tasks of all queries. */
  private List<HiveStep> hiveTasks = new ArrayList<HiveStep>();

  /**
   * Constructs a new HiveRiffle with the given HiveConf object, queries, a list of source taps and a sink.
   *
   * @param driverFactory a factory for creating Driver instances.
   * @param queries       The hive queries to run.
   * @param sources       The source taps of the queries.
   * @param sink          The sink of the queries.
   */
  HiveRiffle( HiveDriverFactory driverFactory, String queries[], Collection<Tap> sources, Tap sink )
    {
    this.driverFactory = driverFactory;
    this.queries = queries;
    if( sources == null || sources.isEmpty() )
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
    if( future != null )
      return;
    // the history is used in the hive SessionState, which is kept in a ThreadLocal, so we can only set it, once
    // we are actually running. Otherwise, we break Hive's internal state.
    driverFactory.setCascadingHiveHistoryDecorator( this );

    ExecutorService executorService = Executors.newSingleThreadExecutor( new HiveFlowThreadFactory() );
    Callable<Throwable> queryRunner = new HiveQueryRunner( driverFactory, queries );
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
    return sources;
    }

  @ProcessConfiguration
  public Configuration getConfiguration()
    {
    return driverFactory.getHiveConf();
    }

  @ProcessCounters
  public Map<String, Map<String, Long>> getCounters()
    {
    // roll up all counters
    Map<String, Map<String, Long>> counters = new HashMap<String, Map<String, Long>>();

    for( HiveStep internalHiveTask : hiveTasks )
      {
      for( Map.Entry<String, Map<String, Long>> entry : internalHiveTask.getCounters().entrySet() )
        {
        if( !counters.containsKey( entry.getKey() ) )
          counters.put( entry.getKey(), entry.getValue() );
        else
          {
          Map<String, Long> existing = counters.get( entry.getKey() );
          for( Map.Entry<String, Long> ctr : entry.getValue().entrySet() )
            {
            if( !existing.containsKey( ctr.getKey() ) )
              existing.put( ctr.getKey(), ctr.getValue() );
            else
              existing.put( ctr.getKey(), existing.get( entry.getKey() ) + ctr.getValue() );
            }
          }
        }
      }
    return counters;
    }

  @ProcessChildren
  public List<Object> getChildren()
    {
    return Collections.<Object>unmodifiableList( hiveTasks );
    }

  private Map<String, Map<String, Long>> asMap( Counters counters )
    {
    Map<String, Map<String, Long>> groups = new LinkedHashMap<String, Map<String, Long>>();

    for( String groupName : counters.getGroupNames() )
      {
      Map<String, Long> group = new HashMap<String, Long>();
      Counters.Group counterGroup = counters.getGroup( groupName );
      for( Counter counter : counterGroup )
        group.put( counter.getName(), counter.getValue() );

      groups.put( groupName, group );
      }
    return groups;
    }

  @Override
  public String getHistFileName()
    {
    return original.getHistFileName();
    }

  @Override
  public void startQuery( String cmd, String id )
    {
    original.startQuery( cmd, id );
    }

  @Override
  public void setQueryProperty( String queryId, Keys propName, String propValue )
    {
    original.setQueryProperty( queryId, propName, propValue );
    }

  @Override
  public void setTaskProperty( String queryId, String taskId, Keys propName, String propValue )
    {
    original.setTaskProperty( queryId, taskId, propName, propValue );
    }

  @Override
  public synchronized void setTaskCounters( String queryId, String taskId, Counters ctrs )
    {
    // task id is something like "step-1" and can repeat, so we concat it with the unique query id.
    hiveTasks.add( new HiveStep( queryId + " " + taskId, asMap( ctrs ) ) );
    original.setTaskCounters( queryId, taskId, ctrs );
    }

  @Override
  public void printRowCount( String queryId )
    {
    original.printRowCount( queryId );
    }

  @Override
  public void endQuery( String queryId )
    {
    original.endQuery( queryId );
    }

  @Override
  public void startTask( String queryId, Task<? extends Serializable> task, String taskName )
    {
    original.startTask( queryId, task, taskName );
    }

  @Override
  public void endTask( String queryId, Task<? extends Serializable> task )
    {
    original.endTask( queryId, task );
    }

  @Override
  public void progressTask( String queryId, Task<? extends Serializable> task )
    {
    original.progressTask( queryId, task );
    }

  @Override
  public void logPlanProgress( QueryPlan plan ) throws IOException
    {
    original.logPlanProgress( plan );
    }

  @Override
  public void setIdToTableMap( Map<String, String> map )
    {
    original.setIdToTableMap( map );
    }

  @Override
  public void closeStream()
    {
    original.closeStream();
    }

  public void setOriginal( HiveHistory original )
    {
    this.original = original;
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
