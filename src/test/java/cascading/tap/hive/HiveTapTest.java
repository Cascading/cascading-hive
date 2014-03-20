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

package cascading.tap.hive;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import cascading.CascadingException;
import cascading.HiveTestCase;
import cascading.scheme.NullScheme;
import cascading.tap.SinkMode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

/**
 * Tests for HiveTap.
 */
public class HiveTapTest extends HiveTestCase
  {

  @Test
  public void testResourceExistsWithNonExistingTable() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( createHiveConf(), desc, new NullScheme() );
    assertFalse( tap.resourceExists( new JobConf() ) );
    }

  @Test
  public void testCreateResource() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( createHiveConf(), desc, new NullScheme() );
    assertTrue( tap.createResource( new JobConf() ) );
    assertTrue( tap.resourceExists( new JobConf() ) );
    assertNotNull( tap.getPath() );
    }

  @Test(expected = HiveTableValidationException.class)
  public void testResourceExistsStrictModeColumnCountMismatch() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( createHiveConf(), desc, new NullScheme() );
    tap.createResource( new JobConf() );

    HiveTableDescriptor mismatch = new HiveTableDescriptor( "myTable", new String[]{"key", "value"},
      new String[]{"string", "string"} );

    tap = new HiveTap( createHiveConf(), mismatch, new NullScheme(  ), SinkMode.REPLACE, true );
    tap.resourceExists( new JobConf(  ) );

    }

  @Test(expected = HiveTableValidationException.class)
  public void testResourceExistsStrictModeNameMismatch() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( createHiveConf(), desc, new NullScheme() );
    tap.createResource( new JobConf() );

    HiveTableDescriptor mismatch = new HiveTableDescriptor( "myTable", new String[]{"key2"}, new String[]{"string"} );

    tap = new HiveTap( createHiveConf(), mismatch, new NullScheme(  ), SinkMode.REPLACE, true );
    tap.resourceExists( new JobConf(  ) );

    }

  @Test(expected = HiveTableValidationException.class)
  public void testResourceExistsStrictModeTypeMismatch() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( createHiveConf(), desc, new NullScheme() );
    tap.createResource( new JobConf() );

    HiveTableDescriptor mismatch = new HiveTableDescriptor( "myTable", new String[]{"key"}, new String[]{"int"} );
    tap = new HiveTap( createHiveConf(), mismatch, new NullScheme(  ), SinkMode.REPLACE, true );
    tap.resourceExists( new JobConf(  ) );
    }




  }
