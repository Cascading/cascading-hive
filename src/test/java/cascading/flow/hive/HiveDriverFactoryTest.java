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

import java.util.Collections;
import java.util.Map;

import cascading.HiveTestCase;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Test;

/**
 * Tests for HiveDriverFactory.
 */
public class HiveDriverFactoryTest extends HiveTestCase
  {
  @Test
  public void testCreateHiveDriver()
    {
    Driver driver = new HiveDriverFactory().createHiveDriver();
    assertNotNull( driver );
    driver.destroy();
    }

  @Test
  public void testAddingProperties()
    {
    String TESTVAL = "10";
    Map<String, String> properties = Collections.singletonMap( ConfVars.DYNAMICPARTITIONMAXPARTS.varname, TESTVAL );
    Driver driver = new HiveDriverFactory( properties ).createHiveDriver();
    SessionState session = SessionState.get();
    HiveConf conf = session.getConf();
    driver.destroy();

    assertEquals( TESTVAL, conf.getVar( ConfVars.DYNAMICPARTITIONMAXPARTS ) );
    }
  }
