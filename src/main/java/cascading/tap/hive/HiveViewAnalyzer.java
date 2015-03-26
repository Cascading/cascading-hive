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

package cascading.tap.hive;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.hive.HiveDriverFactory;
import cascading.scheme.NullScheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HiveViewAnalyzer can take a query representing a Hive view and find the underlying tables on HDFS. This works in a
 * recursive way so that views, based on views, based on views etc. can be deconstructed into the participating tables.
 * This is useful when running a query on a view, which is meant to participate in a Cascade.
 */
public class HiveViewAnalyzer implements Closeable
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HiveViewAnalyzer.class );
  /** a stack of views. */
  private Deque<String> viewDefinitions;
  /** The Taps (Hive tables), which represent the view. */
  private Set<Tap> taps;
  /** The Hive driver to use. */
  private Driver driver;

  /**
   * Constructs a new HiveViewAnalyzer object.
   */
  public HiveViewAnalyzer()
    {
    viewDefinitions = new ArrayDeque<String>();
    taps = new HashSet<Tap>();
    driver = new HiveDriverFactory().createHiveDriver();
    }

  /**
   * Analyzes a query involving Views and returns a Collection of Taps, which are used to compose the view.
   *
   * @param query The hive query to analyze.
   * @return a Collection of Taps.
   */
  public Collection<Tap> asTaps( String query )
    {
    viewDefinitions.push( query );
    while( !viewDefinitions.isEmpty() )
      {
      String sql = viewDefinitions.pop();
      LOG.debug( "compiling view definition '{}'", sql );
      int result = driver.compile( sql );
      if( result != 0 )
        {
        throw new CascadingException( "unable to compile query '" + sql  + "'. Make sure that all tables in the view are" +
          "already registered in the Hive MetaStore ");
        }
      // the plan and the schema contain what we need to build proper taps/schemes/fields
      QueryPlan plan = driver.getPlan();
      extractTaps( plan );
      }
    return taps;
    }

  /**
   * Constructs Tap instances based on the paths on HDFS for the underlying tables.
   *
   * @param plan The hive plan to analyze.
   */
  private void extractTaps( QueryPlan plan )
    {
    Iterator<ReadEntity> inputIterator = plan.getInputs().iterator();
    while( inputIterator.hasNext() )
      {
      ReadEntity readEntity = inputIterator.next();
      Table table = readEntity.getT();
      if( table.isView() )
        {
        // drill deeper into the view definitions
        LOG.debug( "found nested view with definition '{}'", table.getViewExpandedText() );
        viewDefinitions.push( table.getViewExpandedText() );
        }
      else
        {
        try
          {
          if( readEntity.getLocation() != null )
            {
            String inputPath = readEntity.getLocation().getPath();
            taps.add( new Hfs( new NullScheme(), inputPath, SinkMode.KEEP ) );
            }
          }
        catch( Exception exception )
          {
          throw new CascadingException( exception );
          }
        }
      }
    }

  @Override
  public void close()
    {
    driver.close();
    }

  }
