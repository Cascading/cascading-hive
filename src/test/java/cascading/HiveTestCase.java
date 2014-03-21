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

package cascading;

import java.io.File;
import java.io.IOException;

import cascading.flow.hive.HiveDriverFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.BeforeClass;

/**
 * Super class for tests interacting with hive and its MetaStore.
 */
public class HiveTestCase extends PlatformTestCase
  {
  public final static File DERBY_HOME = new File( "build/test/derby" );

  public final static String CWD = System.getProperty( "user.dir" );

  public final static String HIVE_WAREHOUSE_DIR = CWD + "/build/test/hive";

  private HiveDriverFactory hiveDriverFactory = new HiveDriverFactoryForTesting( createHiveConf() );

  @BeforeClass
  public static void beforeClass() throws IOException
    {
    // do this once per class, otherwise we run into bizarre derby errors
    if( DERBY_HOME.exists() )
      FileUtils.deleteDirectory( DERBY_HOME );
    DERBY_HOME.mkdirs();
    System.setProperty( "derby.system.home", DERBY_HOME.getAbsolutePath() );
    }

  /**
   * Creates a HiveConf object usable for testing.
   * @return a HiveConf object.
   */
  public HiveConf createHiveConf()
    {
    HiveConf conf = new HiveConf();
    conf.set( HiveConf.ConfVars.METASTOREWAREHOUSE.varname, HIVE_WAREHOUSE_DIR );
    return conf;
    }

  public HiveDriverFactory createHiveDriverFactory()
    {
    return hiveDriverFactory;
    }

  }
