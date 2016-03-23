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

package cascading.scheme.hcatalog;

import java.util.Arrays;
import java.util.Set;

import cascading.tuple.Fields;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SchemaUtilsTest
  {

  private static final String FOO = "foo";
  private static final String BAR = "bar";
  private static final String BAZ = "baz";

  private static final HCatFieldSchema FOO_COLUMN;
  private static final HCatFieldSchema BAR_COLUMN;
  private static final HCatFieldSchema BAZ_COLUMN;

  static
    {
    try
      {
      FOO_COLUMN = new HCatFieldSchema( FOO, TypeInfoFactory.stringTypeInfo, null );
      BAR_COLUMN = new HCatFieldSchema( BAR, TypeInfoFactory.stringTypeInfo, null );
      BAZ_COLUMN = new HCatFieldSchema( BAZ, TypeInfoFactory.stringTypeInfo, null );
      }
    catch( HCatException e )
      {
      throw new RuntimeException( e );
      }
    }

  private final HCatSchema partitionColumns = new HCatSchema( Arrays.asList( BAZ_COLUMN ) );
  private final HCatSchema dataColumns = new HCatSchema( Arrays.asList( FOO_COLUMN, BAR_COLUMN ) );

  @Test(expected = IllegalArgumentException.class)
  public void duplicateFields()
    {
    SchemaUtils.getLowerCaseFieldNames( new Fields( FOO.toUpperCase(), FOO ) );
    }

  @Test
  public void typicalLowerCaseNames()
    {
    Set<String> names = SchemaUtils.getLowerCaseFieldNames( new Fields( FOO.toUpperCase() ) );
    assertEquals( 1, names.size() );
    assertEquals( FOO, names.iterator().next() );
    }

  @Test
  public void sourceSchemaAll() throws HCatException
    {
    Fields fields = new Fields( FOO, BAR, BAZ );

    HCatSchema schema = SchemaUtils.getSourceSchema( partitionColumns, dataColumns, fields );

    assertEquals( 3, schema.size() );
    assertEquals( BAZ_COLUMN, schema.get( 0 ) );
    assertEquals( FOO_COLUMN, schema.get( 1 ) );
    assertEquals( BAR_COLUMN, schema.get( 2 ) );
    }

  @Test
  public void sourceSchemaOmitPartitionColumn() throws HCatException
    {
    Fields fields = new Fields( FOO, BAR );

    HCatSchema schema = SchemaUtils.getSourceSchema( partitionColumns, dataColumns, fields );

    assertEquals( 2, schema.size() );
    assertEquals( FOO_COLUMN, schema.get( 0 ) );
    assertEquals( BAR_COLUMN, schema.get( 1 ) );
    }

  @Test
  public void sourceSchemaOmitDataColumn() throws HCatException
    {
    Fields fields = new Fields( FOO, BAZ );

    HCatSchema schema = SchemaUtils.getSourceSchema( partitionColumns, dataColumns, fields );

    assertEquals( 2, schema.size() );
    assertEquals( BAZ_COLUMN, schema.get( 0 ) );
    assertEquals( FOO_COLUMN, schema.get( 1 ) );
    }

  @Test(expected = IllegalArgumentException.class)
  public void sourceSchemaSpecifyNonExistent() throws HCatException
    {
    Fields fields = new Fields( FOO, BAR, BAZ );
    HCatSchema dataColumns = new HCatSchema( Arrays.asList( FOO_COLUMN ) );

    SchemaUtils.getSourceSchema( partitionColumns, dataColumns, fields );
    }

  @Test
  public void sinkSchemaAll() throws HCatException
    {
    Fields fields = new Fields( FOO, BAR, BAZ );

    HCatSchema schema = SchemaUtils.getSinkSchema( partitionColumns, dataColumns, fields );

    assertEquals( 3, schema.size() );
    assertEquals( BAZ_COLUMN, schema.get( 0 ) );
    assertEquals( FOO_COLUMN, schema.get( 1 ) );
    assertEquals( BAR_COLUMN, schema.get( 2 ) );
    }

  @Test(expected = IllegalArgumentException.class)
  public void sinkSchemaOmitPartitionColumn() throws HCatException
    {
    Fields fields = new Fields( FOO, BAR );

    SchemaUtils.getSinkSchema( partitionColumns, dataColumns, fields );
    }

  @Test
  public void sinkSchemaOmitDataColumn() throws HCatException
    {
    Fields fields = new Fields( FOO, BAZ );

    HCatSchema schema = SchemaUtils.getSinkSchema( partitionColumns, dataColumns, fields );

    assertEquals( 2, schema.size() );
    assertEquals( BAZ_COLUMN, schema.get( 0 ) );
    assertEquals( FOO_COLUMN, schema.get( 1 ) );
    }

  @Test(expected = IllegalArgumentException.class)
  public void sinkSchemaSpecifyNonExistent() throws HCatException
    {
    Fields fields = new Fields( FOO, BAR, BAZ );
    HCatSchema dataColumns = new HCatSchema( Arrays.asList( FOO_COLUMN ) );

    SchemaUtils.getSinkSchema( partitionColumns, dataColumns, fields );
    }

  }
