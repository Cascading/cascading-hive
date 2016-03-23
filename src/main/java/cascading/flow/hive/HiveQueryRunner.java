/*
* Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import cascading.CascadingException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for running ad-hoc Hive queries. The class is meant as a convenience class for cases where the hive queries
 * do not fit into the Cascading processing model. It also implements the Callable and Runnable interface so that the
 * queries can be submitted to a ExecutorService.
 */
public class HiveQueryRunner implements Runnable, Callable<Throwable>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HiveQueryRunner.class );

  /** The hive queries to run. */
  private final String queries[];

  /** Result of executed queries. */
  private final List<Object>[] queryResults;

  /** Flag to enable fetching query results. */
  private final boolean fetchQueryResults;

  /** Factory for creating Driver instances. */
  private final HiveDriverFactory driverFactory;

  /**
   * Constructs a new HiveQueryRunner object with the given queries.
   *
   * @param queries The hive queries to run.
   */
  public HiveQueryRunner( String queries[] )
    {
    this( new HiveDriverFactory(), queries );
    }

  /**
   * Constructs a new HiveQueryRunner with the given HiveDriverFactory and queries.
   *
   * @param driverFactory The HiveDriverFactory to use.
   * @param queries       The queries to run.
   */
  HiveQueryRunner( HiveDriverFactory driverFactory, String queries[] )
    {
    this( driverFactory, queries, false );
    }

  /**
   * Constructs a new HiveQueryRunner with the given HiveDriverFactory and queries.
   *
   * @param driverFactory     The HiveDriverFactory to use.
   * @param queries           The queries to run.
   * @param fetchQueryResults {@code true} if the results of each queries must be fetched, {@code false} otherwise.
   */
  @SuppressWarnings("unchecked")
  HiveQueryRunner( HiveDriverFactory driverFactory, String queries[], boolean fetchQueryResults )
    {
    this.driverFactory = driverFactory;
    this.queries = queries;
    this.fetchQueryResults = fetchQueryResults;
    queryResults = fetchQueryResults ? new List[ queries.length ] : null;
    }

  /**
   * Returns an array of query results.
   * <p>
   * Each position of the array is a {@link List} of rows that corresponds to the results of the query at the same
   * position in the given array of queries.
   * </p>
   * <p>
   * This method can only be invoked if {@code fetchQueryResults} was set to {@code true} at construction time.
   * </p>
   *
   * @return A {@link List} of rows each executed query.
   * @throws IllegalStateException If {@code fetchQueryResults} was set to false in the constructor.
   */
  public List<Object>[] getQueryResults()
    {
    if( !fetchQueryResults )
      throw new IllegalStateException( "query results fetch is disabled" );
    return queryResults;
    }

  @Override
  public void run()
    {
    Driver driver = null;
    String currentQuery = null;
    try
      {
      driver = driverFactory.createHiveDriver();
      if( fetchQueryResults )
        Arrays.fill( queryResults, null );
      for( int i = 0; i < queries.length; i++ )
        {
        currentQuery = queries[ i ];
        LOG.info( "running hive query: '{}'", currentQuery );
        CommandProcessorResponse response = driver.run( currentQuery );
        if( response.getResponseCode() != 0 )
          throw new CascadingException( "hive error '" + response.getErrorMessage() + "' while running query " + currentQuery );
        if( fetchQueryResults )
          {
          List<Object> results = new LinkedList<>();
          if( !driver.getResults( results ) )
            {
            results = null;
            LOG.info( "no results returned for hive query '{}'", currentQuery );
            }
          queryResults[ i ] = results;
          }
        }
      }
    catch( CommandNeedRetryException exception )
      {
      if( currentQuery == null )
        throw new CascadingException( "problem while executing hive queries: " + Arrays.toString( queries ), exception );
      else
        throw new CascadingException( "problem while executing hive query: " + currentQuery, exception );
      }
    catch( IOException exception )
      {
      throw new CascadingException( "problem while fetching the results of hive query: " + currentQuery, exception );
      }
    finally
      {
      if( driver != null )
        driver.destroy();
      }
    }

  @Override
  public Throwable call() throws Exception
    {
    try
      {
      this.run();
      }
    catch( Throwable throwable )
      {
      return throwable;
      }
    return null;
    }
  }
