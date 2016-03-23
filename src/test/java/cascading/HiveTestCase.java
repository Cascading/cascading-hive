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

package cascading;

import java.io.File;
import java.io.IOException;
import java.util.List;

import cascading.flow.hive.HiveDriverFactory;
import cascading.flow.hive.HiveQueryRunner;
import cascading.flow.hive.HiveQueryRunnerForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * Super class for tests interacting with hive and its MetaStore.
 */
abstract public class HiveTestCase extends PlatformTestCase
  {
  private static final long serialVersionUID = 1L;

  public final static File DERBY_HOME = new File( "build/test/derby" );

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  private HiveDriverFactory hiveDriverFactory;

  private HiveConf hiveConf;

  @BeforeClass
  public static void createDerbyFolder() throws IOException
    {
    // do this once per class, otherwise we run into bizarre derby errors
    if( DERBY_HOME.exists() )
      FileUtils.deleteDirectory( DERBY_HOME );
    DERBY_HOME.mkdirs();
    System.setProperty( "derby.system.home", DERBY_HOME.getAbsolutePath() );
    }

  @Before
  public void before()
    {
    System.setProperty( HiveConf.ConfVars.METASTOREWAREHOUSE.varname, dbFolder.getRoot().getAbsolutePath() );
    hiveDriverFactory = new HiveDriverFactoryForTesting( createHiveConf() );
    }

  /**
   * Creates a HiveConf object usable for testing.
   *
   * @return a HiveConf object.
   */
  public HiveConf createHiveConf()
    {
    if( hiveConf == null )
      {
      hiveConf = new HiveConf();
      hiveConf.set( "fs.raw.impl", RawFileSystem.class.getName() );
      hiveConf.set( HiveConf.ConfVars.METASTOREWAREHOUSE.varname, dbFolder.getRoot().getAbsolutePath() );
      }
    return hiveConf;
    }

  /**
   * Method for running ad-hoc query in tests.
   *
   * @param query
   * @return query results
   */
  public List<Object> runHiveQuery( String query )
    {
    HiveQueryRunner runner = new HiveQueryRunnerForTesting( hiveDriverFactory, new String[]{query}, true );
    runner.run();
    return runner.getQueryResults()[ 0 ];
    }

  /**
   * Creates a new HiveDriverFactory for testing
   */
  public HiveDriverFactory createHiveDriverFactory()
    {
    return hiveDriverFactory;
    }

  /**
   * Creates a new IMetaStoreClient for interacting with the MetaStore created in tests.
   */
  public IMetaStoreClient createMetaStoreClient() throws MetaException
    {
    return RetryingMetaStoreClient.getProxy( createHiveConf(),
      new HiveMetaHookLoader()
        {
        @Override
        public HiveMetaHook getHook( Table tbl ) throws MetaException
          {
          return null;
          }
        }, HiveMetaStoreClient.class.getName()
    );
    }

  }
