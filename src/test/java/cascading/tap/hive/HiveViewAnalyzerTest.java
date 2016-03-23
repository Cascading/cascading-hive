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

package cascading.tap.hive;

import java.util.Collection;

import cascading.HiveTestCase;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import org.junit.Test;

/**
 *
 */
public class HiveViewAnalyzerTest extends HiveTestCase
  {

  @Test
  public void testAsTaps()
    {
    runHiveQuery( "create table foobar (key string, value string) " );
    HiveViewAnalyzer analyzer = new HiveViewAnalyzer();
    Collection<Tap> taps = analyzer.asTaps( "select key from foobar" );
    assertEquals( 1, taps.size() );
    Hfs tap = (Hfs) taps.iterator().next();
    assertEquals( dbFolder.getRoot().getAbsolutePath() + "/foobar", tap.getPath().toString() );
    runHiveQuery( "create view firstView as select key, value from foobar where key = '42' " );
    taps = analyzer.asTaps( "select key from firstView" );
    tap = (Hfs) taps.iterator().next();
    assertEquals( dbFolder.getRoot().getAbsolutePath() + "/foobar", tap.getPath().toString() );
    runHiveQuery( "create view secondView as select key, value from firstView where value = 'magic' " );
    taps = analyzer.asTaps( "select key from firstView" );
    tap = (Hfs) taps.iterator().next();
    assertEquals( dbFolder.getRoot().getAbsolutePath() + "/foobar", tap.getPath().toString() );
    }

  }
