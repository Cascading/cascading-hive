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

import cascading.CascadingException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for running ad-hoc Hive queries. The class is meant as a convenience class for cases where a hive query
 * does not fit into the Cascading processing model. It also implements the Runnable interface so that a query can be
 * submitted to a CompletionService.
 */
public class HiveQueryRunner implements Runnable
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HiveQueryRunner.class );

  /** The hive query to run. */
  private final String query;

  /** Factory for creating Driver instances. */
  private final HiveDriverFactory driverFactory;

  /**
   * Constructs a new HiveQueryRunner object with the given query.
   *
   * @param query The hive query to run.
   */
  public HiveQueryRunner( String query )
    {
    this( new HiveDriverFactory(), query );
    }

  /**
   * Constructs a new HiveQueryRunner with the given HiveDriverFactory and query.
   *
   * @param driverFactory The HiveDriverFactory to use.
   * @param query         The query to run.
   */
  HiveQueryRunner( HiveDriverFactory driverFactory, String query )
    {
    this.driverFactory = driverFactory;
    this.query = query;
    }

  @Override
  public void run()
    {
    Driver driver = null;
    try
      {
      driver = driverFactory.createHiveDriver();
      LOG.info( "running hive query: '{}'", query );
      CommandProcessorResponse response = driver.run( this.query );
      if( response.getResponseCode() != 0 )
        throw new CascadingException( "hive error '" + response.getErrorMessage() + "' while running query " + query );
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
  }
