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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import cascading.CascadingException;
import cascading.flow.FlowDescriptors;
import cascading.flow.process.ProcessFlow;
import cascading.tap.Tap;
import cascading.tap.hive.HiveNullTap;
import cascading.tap.hive.HivePartitionTap;
import cascading.tap.hive.HiveTap;
import org.apache.commons.lang.StringUtils;

/**
 * A subclass of ProcessFlow for running Hive queries.
 */
public class HiveFlow extends ProcessFlow
  {
  /**
   * Constructs a new HiveFlow object with the given name, queries, a list of source taps and sink.
   *
   * @param name    The name of the flow.
   * @param query The hive query to run.
   * @param sources The source taps of the queries.
   * @param sink    The sink of the queries.
   * @param properties Properties to add to the hive conf when running the query such as performance options.
   */
   public HiveFlow( String name, String query, Collection<Tap> sources, Tap sink, Map<String, String> properties )
    {
    this( name, new HiveDriverFactory(properties), new String[]{query}, sources, sink );
    }

  /**
   * Constructs a new HiveFlow object with the given name, queries, a list of source taps. This constructor can be
   * used when the Flow does not really have
   *
   * @param name    The name of the flow.
   * @param query The hive query to run.
   * @param sources The source taps of the queries.
   */
  public HiveFlow( String name, String query, Collection<Tap> sources )
    {
    this( name, new HiveDriverFactory(), new String[]{query}, sources, HiveNullTap.DEV_NULL );
    }

  /**
   * Constructs a new HiveFlow object with the given name, queries, a list of source taps and sink.
   *
   * @param name    The name of the flow.
   * @param queries The hive queries to run.
   * @param sources The source taps of the queries.
   * @param sink    The sink of the queries.
   */
  public HiveFlow( String name, String queries[], Collection<Tap> sources, Tap sink )
    {
    this( name, new HiveDriverFactory(), queries, sources, sink );
    }

  /**
   * Constructs a new HiveFlow object with the given name, queries, a list of source taps and sink.
   *
   * @param name       The name of the flow.
   * @param queries    The hive queries to run.
   * @param sources    The source taps of the queries.
   * @param sink       The sink of the queries.
   * @param properties Properties to add to the hive conf when running the query such as performance options.
   */
  public HiveFlow( String name, String queries[], Collection<Tap> sources, Tap sink, Map<String, String> properties )
    {
    this( name, new HiveDriverFactory(properties), queries, sources, sink );
    }

  /**
   * Constructs a new HiveFlow object with the given name, queries, a list of source taps. This constructor can be
   * used when the Flow does not really have
   *
   * @param name    The name of the flow.
   * @param queries The hive queries to run.
   * @param sources The source taps of the queries.
   */
  public HiveFlow( String name, String queries[], Collection<Tap> sources )
    {
    this( name, new HiveDriverFactory(), queries, sources, HiveNullTap.DEV_NULL );
    }

  /**
   * Package private constructor which allows injecting a custom HiveDriverFactory during tests.
   */
  HiveFlow( String name, HiveDriverFactory driverFactory, String query, Collection<Tap> sources, Tap sink )
    {
    this( name, driverFactory, new String[]{query}, sources, sink );
    }

  /**
   * Package private constructor which allows injecting a custom HiveDriverFactory during tests.
   */
  HiveFlow( String name, HiveDriverFactory driverFactory, String queries[], Collection<Tap> sources, Tap sink )
    {
    super( new Properties(), name, new HiveRiffle( driverFactory, queries, sources, sink ), createFlowDescriptor( queries ) );
    }

  private static Map<String, String> createFlowDescriptor( String[] queries )
    {
    Map<String, String> flowDescriptor = new LinkedHashMap<String, String>();
    flowDescriptor.put( FlowDescriptors.STATEMENTS, StringUtils.join( queries, FlowDescriptors.VALUE_SEPARATOR ) );
    flowDescriptor.put( FlowDescriptors.DESCRIPTION, "Hive flow" );
    return flowDescriptor;
    }

  @Override
  public void start()
    {
    registerSinkTable();
    super.start();
    }

  @Override
  public void complete()
    {
    registerSinkTable();
    super.complete();
    }

  private void registerSinkTable()
    {
    try
      {
      Tap sink = getSink();
      if ( sink instanceof HiveTap || sink instanceof HivePartitionTap )
        sink.createResource( getFlowProcess() );
      }
    catch( IOException exception )
      {
      throw new CascadingException( exception );
      }
    }
  }
