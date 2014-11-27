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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tuple.Fields;
import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the HiveTableDescriptor.
 */
public class HiveTableDescriptorTest
  {

  @Test(expected = IllegalArgumentException.class)
  public void testConstructionTableNameNull()
    {
    new HiveTableDescriptor( null, new String[]{"foo"}, new String[]{"string"} );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructionTableNameEmpty()
    {
    new HiveTableDescriptor( "", new String[]{"foo"}, new String[]{"string"} );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructionColumnNamesAndTypesMismatch()
    {
    new HiveTableDescriptor( "table", new String[]{"foo"}, new String[]{} );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructionPartitionKeysMismatch()
    {
    new HiveTableDescriptor( "table", new String[]{"foo"}, new String[]{}, new String[]{"bar"} );
    }

  @Test
  public void testToHiveTable()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "myTable", new String[]{"key", "value"},
      new String[]{"int", "STRING"} );
    Table table = descriptor.toHiveTable();
    assertEquals( "mytable", table.getTableName() );
    assertEquals( MetaStoreUtils.DEFAULT_DATABASE_NAME, table.getDbName() );
    assertEquals( TableType.MANAGED_TABLE.toString(), table.getTableType() );

    StorageDescriptor sd = table.getSd();
    assertNotNull( sd );

    List<FieldSchema> expectedSchema = Arrays.asList( new FieldSchema( "key", "int", "created by Cascading" ),
      new FieldSchema( "value", "string", "created by Cascading" ) );
    assertEquals( expectedSchema, sd.getCols() );

    SerDeInfo serDeInfo = sd.getSerdeInfo();
    assertEquals( HiveTableDescriptor.HIVE_DEFAULT_SERIALIZATION_LIB_NAME, serDeInfo.getSerializationLib() );

    assertEquals( HiveTableDescriptor.HIVE_DEFAULT_INPUT_FORMAT_NAME, sd.getInputFormat() );
    assertEquals( HiveTableDescriptor.HIVE_DEFAULT_OUTPUT_FORMAT_NAME, sd.getOutputFormat() );
    assertFalse( table.isSetPartitionKeys() );
    }


  @Test
  public void testToHiveTableWithPartitioning()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "mytable", new String[]{"key", "value"},
      new String[]{"int", "string"}, new String[]{"key"} );
    assertTrue( descriptor.isPartitioned() );
    Table table = descriptor.toHiveTable();
    assertEquals( "mytable", table.getTableName() );
    assertEquals( MetaStoreUtils.DEFAULT_DATABASE_NAME, table.getDbName() );

    StorageDescriptor sd = table.getSd();
    assertNotNull( sd );

    List<FieldSchema> expectedSchema = Arrays.asList( new FieldSchema( "value", "string", "created by Cascading" ) );
    assertEquals( expectedSchema, sd.getCols() );

    SerDeInfo serDeInfo = sd.getSerdeInfo();
    assertEquals( HiveTableDescriptor.HIVE_DEFAULT_SERIALIZATION_LIB_NAME, serDeInfo.getSerializationLib() );

    assertEquals( HiveTableDescriptor.HIVE_DEFAULT_INPUT_FORMAT_NAME, sd.getInputFormat() );
    assertEquals( HiveTableDescriptor.HIVE_DEFAULT_OUTPUT_FORMAT_NAME, sd.getOutputFormat() );
    assertTrue( table.isSetPartitionKeys() );

    }

  @Test
  public void testToFields()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "mytable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"} );
    assertEquals( new Fields( "one", "two", "three" ), descriptor.toFields() );
    }

  @Test
  public void testToFieldsWithPartitionedTable()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "mytable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"}, new String[] {"three"});
    assertEquals( new Fields( "one", "two" ), descriptor.toFields() );
    }


  @Test
  public void testToSchemeWithDefaultDelimiter()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "mytable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"} );
    Scheme scheme = descriptor.toScheme();
    assertNotNull( scheme );
    Assert.assertEquals( HiveTableDescriptor.HIVE_DEFAULT_DELIMITER, ( (TextDelimited) scheme ).getDelimiter() );
    }

  @Test
  public void testToSchemeWithCustomDelimiter()
    {
    String delim = "\\t";
    HiveTableDescriptor descriptor = new HiveTableDescriptor( HiveTableDescriptor.HIVE_DEFAULT_DATABASE_NAME, "mytable",
      new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"}, new String[]{},
      delim, HiveTableDescriptor.HIVE_DEFAULT_SERIALIZATION_LIB_NAME, null );
    Scheme scheme = descriptor.toScheme();
    assertNotNull( scheme );
    Assert.assertEquals( delim, ( (TextDelimited) scheme ).getDelimiter() );
    }

  @Test
  public void testCustomDelimiterInSerdeParameters()
    {
    String delim = "\\t";
    HiveTableDescriptor descriptor = new HiveTableDescriptor( HiveTableDescriptor.HIVE_DEFAULT_DATABASE_NAME, "mytable",
      new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"}, new String[]{},
      delim, HiveTableDescriptor.HIVE_DEFAULT_SERIALIZATION_LIB_NAME, null );
    StorageDescriptor sd = descriptor.toHiveTable().getSd();
    Map<String, String> expected = new HashMap<String, String>(  );
    expected.put( "field.delim", delim );
    expected.put( "serialization.format", delim );
    assertEquals( expected, sd.getSerdeInfo().getParameters() );
    }


  @Test
  public void testToSchemeWithNullDelimiter()
    {
    String delim = null;
    HiveTableDescriptor descriptor = new HiveTableDescriptor( HiveTableDescriptor.HIVE_DEFAULT_DATABASE_NAME, "mytable",
      new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"}, new String[]{},
      delim, HiveTableDescriptor.HIVE_DEFAULT_SERIALIZATION_LIB_NAME, null
    );
    Scheme scheme = descriptor.toScheme();
    assertNotNull( scheme );
    Assert.assertEquals( HiveTableDescriptor.HIVE_DEFAULT_DELIMITER, ( (TextDelimited) scheme ).getDelimiter() );
    }

  @Test
  public void testHashCodeEquals()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "mytable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"} );

    HiveTableDescriptor descriptor2 = new HiveTableDescriptor( "MYTABLE", new String[]{"ONE", "two", "three"},
      new String[]{"int", "string", "boolean"} );

    HiveTableDescriptor descriptor3 = new HiveTableDescriptor( "myTable", new String[]{"one", "three", "two"},
      new String[]{"int", "BOOLEAN", "string"} );

    assertEquals( descriptor, descriptor2 );
    assertFalse( descriptor.equals( descriptor3 ) );
    assertFalse( descriptor2.equals( descriptor3 ) );

    Set<HiveTableDescriptor> descriptors = new HashSet<HiveTableDescriptor>();
    descriptors.add( descriptor );
    assertEquals( 1, descriptors.size() );
    descriptors.add( descriptor2 );
    assertEquals( 1, descriptors.size() );
    descriptors.add( descriptor3 );
    assertEquals( 2, descriptors.size() );
    }


  @Test
  public void testToString()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "mytable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"} );
    assertNotNull( descriptor.toString() );
    }

  @Test
  public void testGetFilesystempathWithDefaultDB()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "myTable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"} );
    assertEquals( "warehouse/mytable", descriptor.getLocation( "warehouse") );
    }

  @Test
  public void testGetFilesystempathWithCustomDB()
    {
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "myDB", "myTable", new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"} );
    assertEquals( "warehouse/mydb.db/mytable", descriptor.getLocation( "warehouse" ) );
    }

  @Test
  public void testGetFilesystempathWithSpecifiedLocation()
      {
      HiveTableDescriptor descriptor = new HiveTableDescriptor( HiveTableDescriptor.HIVE_DEFAULT_DATABASE_NAME, "mytable",
        new String[]{"one", "two", "three"},
        new String[]{"int", "string", "boolean"}, new String[]{},
        ",", HiveTableDescriptor.HIVE_DEFAULT_SERIALIZATION_LIB_NAME, new Path( "file:/custom_path" ) );

      assertEquals( "file:/custom_path", descriptor.getLocation( "warehouse" ) );
      }
  }

