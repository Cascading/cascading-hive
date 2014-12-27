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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import cascading.util.Util;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * A simple factory class for creating Driver instances. This class exists to make
 * testing and stubbing within the HiveRiffle easier.
 */
public class HiveDriverFactory implements Serializable
  {
  /** Properties to merge into the hive conf. */
  protected Map<String, String> properties;

  /** a HiveConf object. */
  protected transient HiveConf hiveConf;

  /** decorator for the HiveHistory object to fetch counters.*/
  private CascadingHiveHistoryDecorator hiveHistory;

  /** Initialises the HiveDriverFactory. */
  public HiveDriverFactory()
    {
    this( Collections.<String, String>emptyMap() );
    }

  /**
   * Initialises the HiveDriverFactory with the specified properties applied to the HiveConf for the driver.
   *
   * @param properties Properties to add to the HiveConf used by the driver.
   */
  public HiveDriverFactory( Map<String, String> properties )
    {
    this.properties = properties;
    }

  /**
   * Creates a new Driver instance and sets everything up for compiling/processing queries. Users of
   * this method are responsible for destroying the driver instance, after they are done.
   *
   * @return a new Driver instance.
   */
  public Driver createHiveDriver( )
    {
    SessionState sessionState = SessionState.start( getHiveConf() );
    if( hiveHistory != null )
      {
      HiveHistory original = sessionState.getHiveHistory();
      hiveHistory.setOriginal( original );
      // this might break in the future, but there is no API to do that, so we have to use reflection.
      Util.setInstanceFieldIfExists( sessionState, "hiveHist", hiveHistory );
      }
    SessionState.setCurrentSessionState( sessionState );
    Driver driver = new Driver( getHiveConf() );
    driver.init();
    return driver;
    }

  /**
   * Private helper method to return the HiveConf object.
   * @return the HiveConf object.
   */
  private HiveConf getHiveConf()
    {
    if ( this.hiveConf == null )
      {
      this.hiveConf = new HiveConf();
      for ( Map.Entry<String, String> entry : properties.entrySet() )
        this.hiveConf.set( entry.getKey(), entry.getValue() );
      }
    return this.hiveConf;
    }

  /**
   * Sets the CascadingHiveHistoryDecorator.
   * @param hiveHistory a CascadingHiveHistoryDecorator instance.
   */
  public void setCascadingHiveHistoryDecorator( CascadingHiveHistoryDecorator hiveHistory )
    {
    this.hiveHistory = hiveHistory;
    }
  }
