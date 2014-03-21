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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cascading.CascadingException;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tuple.Fields;
import junit.framework.Assert;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the HiveTableDescriptor.
 */
public class HiveTableDescriptorTest
  {

  @Test(expected = CascadingException.class)
  public void testConstructionTableNameNull()
    {
    new HiveTableDescriptor( null, new String[]{"foo"}, new String[]{"string"} );
    }

  @Test(expected = CascadingException.class)
  public void testConstructionTableNameEmpty()
    {
    new HiveTableDescriptor( "", new String[]{"foo"}, new String[]{"string"} );
    }

  @Test(expected = CascadingException.class)
  public void testConstructionColumnNamesAndTypesMismatch()
    {
    new HiveTableDescriptor( "table", new String[]{"foo"}, new String[]{} );
    }

  @Test
  public void testToHiveTable()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "myTable", new String[]{"key", "value"},
      new String[]{"int", "string"} );
    Table table = descriptor.toHiveTable();
    assertEquals( "myTable", table.getTableName() );
    assertEquals( MetaStoreUtils.DEFAULT_DATABASE_NAME, table.getDbName() );

    StorageDescriptor sd = table.getSd();
    assertNotNull( sd );

    List<FieldSchema> expectedSchema = Arrays.asList( new FieldSchema( "key", "int", "" ),
      new FieldSchema( "value", "string", "" ) );
    assertEquals( expectedSchema, sd.getCols() );

    SerDeInfo serDeInfo = sd.getSerdeInfo();
    assertEquals( HiveConf.ConfVars.HIVESCRIPTSERDE.defaultVal, serDeInfo.getSerializationLib() );

    assertEquals( HiveTableDescriptor.HIVE_DEFAULT_INPUT_FORMAT_NAME, sd.getInputFormat() );
    assertEquals( HiveTableDescriptor.HIVE_DEFAULT_OUTPUT_FORMAT_NAME, sd.getOutputFormat() );
    }

  @Test
  public void testToFields()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "myTable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"} );
    assertEquals( new Fields( "one", "two", "three" ), descriptor.toFields() );
    }

  @Test
  public void testToSchemeWithDefaultDelimiter()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "myTable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"} );
    Scheme scheme = descriptor.toScheme();
    assertNotNull( scheme );
    Assert.assertEquals( HiveTableDescriptor.HIVE_DEFAULT_DELIMITER, ( (TextDelimited) scheme ).getDelimiter() );
    }

  @Test
  public void testToSchemeWithCustomDelimiter()
    {
    String delim = "\\t";
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "myTable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"}, delim, HiveTableDescriptor.HIVE_DEFAULT_SERIALIZATION_LIB_NAME );
    Scheme scheme = descriptor.toScheme();
    assertNotNull( scheme );
    Assert.assertEquals( delim, ( (TextDelimited) scheme ).getDelimiter() );
    }

  @Test
  public void testToSchemeWithNullDelimiter()
    {
    String delim = null;
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "myTable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"}, delim, HiveTableDescriptor.HIVE_DEFAULT_SERIALIZATION_LIB_NAME );
    Scheme scheme = descriptor.toScheme();
    assertNotNull( scheme );
    Assert.assertEquals( HiveTableDescriptor.HIVE_DEFAULT_DELIMITER, ( (TextDelimited) scheme ).getDelimiter() );
    }

  @Test
  public void testHashCodeEquals()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "myTable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"} );

    HiveTableDescriptor descriptor2 = new HiveTableDescriptor( "myTable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"} );

    HiveTableDescriptor descriptor3 = new HiveTableDescriptor( "myTable", new String[]{"one","three","two"},
      new String[]{"int", "boolean", "string"} );

    assertEquals( descriptor, descriptor2 );
    assertFalse( descriptor.equals( descriptor3 ));
    assertFalse( descriptor2.equals( descriptor3 ));

    Set<HiveTableDescriptor> descriptors = new HashSet<HiveTableDescriptor>(  );
    descriptors.add( descriptor );
    assertEquals(1, descriptors.size() );
    descriptors.add( descriptor2 );
    assertEquals(1, descriptors.size() );
    descriptors.add( descriptor3 );
    assertEquals(2, descriptors.size() );
    }

  @Test
  public void testToString()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "myTable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"} );
    assertNotNull( descriptor.toString() );
    }
  }
