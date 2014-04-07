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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * Tests for HivePartition
 */
public class HivePartitionTest
  {

  @Test(expected = IllegalArgumentException.class)
  public void testConstructionWithEmptyFields()
    {
    new HivePartition( new Fields(  ) );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructionWithNull()
    {
    new HivePartition( null );
    }

  @Test
  public void testToPartition()
    {
    Fields partFields = new Fields( "year", "month", "day" );
    HivePartition partition =  new HivePartition( partFields );
    Tuple tuple = new Tuple( "2014", "03", "31" );
    TupleEntry te = new TupleEntry( partFields, tuple );
    String converted = partition.toPartition( te );
    assertEquals( "year=2014/month=03/day=31/", converted );
    }

  @Test
  public void testToTuple()
    {
    Fields partFields = new Fields( "year", "month", "day" );
    HivePartition partition =  new HivePartition( partFields );
    TupleEntry te = new TupleEntry( partFields, Tuple.size( 3 ));
    partition.toTuple( "year=2014/month=03/day=31/", te );
    Tuple expected = new Tuple( "2014", "03", "31" );
    assertEquals( expected, te.getTuple() );
    }

  @Test
  public void testToHivePartition()
    {
    String [] columnNames = new String[] { "year", "month", "day", "customer", "event"};
    String [] columnTypes = new String[] { "int", "int", "int", "string", "string" };
    String [] partitionColumns = new String[] {"year", "month", "day"};
    HiveTableDescriptor descriptor = new HiveTableDescriptor( "myDb", "myTable", columnNames, columnTypes, partitionColumns );

    Fields partFields = new Fields( partitionColumns );
    HivePartition partition =  new HivePartition( partFields );

    Partition part = partition.toHivePartition( "year=2014/month=03/day=31/", descriptor );

    assertEquals( descriptor.getTableName(), part.getTableName() );
    assertEquals( descriptor.getDatabaseName(), part.getDbName() );
    assertEquals( Arrays.asList( "2014", "03", "31" ), part.getValues() );
    assertEquals(  descriptor.toHiveTable().getSd(), part.getSd() );
    }


  }
