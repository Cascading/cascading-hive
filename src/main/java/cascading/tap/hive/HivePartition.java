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

package cascading.tap.hive;

import java.util.Arrays;
import java.util.HashMap;

import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

/**
 * Implements a Hive compatible Partition for Cascading.
 */
class HivePartition extends DelimitedPartition
  {

  /** separator of field and value in directory names for hive partitions */
  private final static String EQUALS = "=";

  private String delimiter;

  /**
   * Creates a new HivePartition object based on the given Fields instance.
   *
   * @param partitionFields Fields instance used in the partition.
   */
  public HivePartition( Fields partitionFields )
    {
    super( partitionFields );
    this.delimiter = getPattern().pattern();
    }

  /**
   * Creates a Hive MetaStore Partition object based on this Cascading partition and the table described by the given
   * HiveTableDescriptor.
   *
   * @param partitionString The partition string
   * @param tableDescriptor The HiveTableDescriptor describing the table to which this partition belongs.
   * @return a new Partition object, which can be written to the hive MetaStore.
   */
  Partition toHivePartition( String partitionString, HiveTableDescriptor tableDescriptor )
    {
    int now = (int) ( System.currentTimeMillis() / 1000 );
    //Set the correct location in the storage descriptor
    StorageDescriptor sd = tableDescriptor.toHiveTable().getSd();
    if( sd.getLocation() != null )
      sd.setLocation( sd.getLocation() + "/" + partitionString );

    return new Partition( Arrays.asList( parse( partitionString ) ), tableDescriptor.getDatabaseName(),
      tableDescriptor.getTableName(), now, now, sd,
      new HashMap<String, String>() );
    }

  @Override
  public void toTuple( String partition, TupleEntry tupleEntry )
    {
    if( partition.startsWith( delimiter ) )
      partition = partition.substring( 1 );
    tupleEntry.setCanonicalValues( parse( partition ) );
    }

  @Override
  public String toPartition( TupleEntry tupleEntry )
    {
    StringBuilder builder = new StringBuilder();
    Fields fields = getPartitionFields();
    for( int index = 0; index < fields.size(); index++ )
      {
      String fieldName = fields.get( index ).toString();
      builder.append( fieldName ).append( EQUALS ).append( tupleEntry.getString( fieldName ) ).append( "/" );
      }
    return builder.toString();
    }

  /**
   * Parses a given partition String in the format '/key=value/otherKey=otherValue/' and returns the
   * values as an array of Strings in order of appearance.
   *
   * @param partitionString The string to parse.
   * @return an array of Strings, but never <code>null</code>.
   */
  private String[] parse( String partitionString )
    {
    if( partitionString == null )
      return new String[]{};
    String[] split = getPattern().split( partitionString );

    String[] values = new String[ getPartitionFields().size() ];
    for( int index = 0; index < getPartitionFields().size(); index++ )
      {
      String value = split[ index ].split( EQUALS )[ 1 ];
      values[ index ] = value;
      }
    return values;
    }

  @Override
  public String toString()
    {
    return "HivePartition{" +
      "delimiter='" + delimiter + '\'' +
      ", partitionFields='" + getPartitionFields() + '\'' +
      '}';
    }
  }
