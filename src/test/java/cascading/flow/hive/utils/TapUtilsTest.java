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

package cascading.flow.hive.utils;

import cascading.flow.hive.util.TapUtils;
import cascading.scheme.NullScheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for TapUtils.
 */
public class TapUtilsTest
  {

  @Test
  public void testCreateTapWithUnknownFields()
    {
    Tap tap = TapUtils.createTap( "/some/path", Fields.UNKNOWN, SinkMode.REPLACE );
    assertNotNull( tap );
    assertEquals( new NullScheme(  ), tap.getScheme() );
    assertEquals( SinkMode.REPLACE, tap.getSinkMode() );
    }

  @Test
  public void testCreateTapWithKnownFields()
    {
    Fields fields = new Fields( "one", "two", "three" );
    Tap tap = TapUtils.createTap( "/some/path", fields, SinkMode.REPLACE );
    assertNotNull( tap );
    assertEquals( new TextDelimited( fields, TapUtils.HIVE_DEFAULT_DELIMITER ), tap.getScheme() );
    assertEquals( SinkMode.REPLACE, tap.getSinkMode() );
    }

  @Test
  public void testCreateTapWithDelimiter()
    {
    Fields fields = new Fields( "one", "two", "three" );
    String delimiter = "☃";
    Tap tap = TapUtils.createTap( "/some/path", fields, SinkMode.REPLACE, delimiter );
    assertNotNull( tap );
    assertEquals( new TextDelimited( fields, TapUtils.HIVE_DEFAULT_DELIMITER ), tap.getScheme() );
    assertEquals( SinkMode.REPLACE, tap.getSinkMode() );
    assertEquals( delimiter, ((TextDelimited)tap.getScheme()).getDelimiter() );
    }

  }
