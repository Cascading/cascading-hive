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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cascading.CascadingException;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * HiveTableDescriptor encapsulates information about a table in Hive like the table name, column names, types,
 * partitioning etc. The class can convert the information to Hive specific objects or Cascading specific objects. It
 * acts as a translator of the concepts of a Hive table and the concepts of a Cascading Tap/Scheme.
 */
public class HiveTableDescriptor implements Serializable
  {

  /** default DB in Hive. */
  public final static String HIVE_DEFAULT_DATABASE_NAME = MetaStoreUtils.DEFAULT_DATABASE_NAME;

  /** default delimiter in hive tables */
  public static final String HIVE_DEFAULT_DELIMITER = "\1";

  /** default input format used by Hive */
  public static final String HIVE_DEFAULT_INPUT_FORMAT_NAME = "org.apache.hadoop.mapred.TextInputFormat";

  /** default output format used by Hive */
  public static final String HIVE_DEFAULT_OUTPUT_FORMAT_NAME = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";

  /** default serialization lib name */
  public static final String HIVE_DEFAULT_SERIALIZATION_LIB_NAME = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

  /** columns to be used for partitioning */
  private String[] partitionKeys;

  /** field delimiter in the Hive table */
  private String delimiter;

  /** name of the hive table */
  private String tableName;

  /** name of the database */
  private String databaseName;

  /** names of the columns */
  private String[] columnNames;

  /** hive column types */
  private String[] columnTypes;

  /** Hive serialization library */
  private String serializationLib;

  /** Optional alternate location of the table */
  private String location = null;

  /**
   * Constructs a new HiveTableDescriptor object.
   *
   * @param tableName   The table name.
   * @param columnNames Names of the columns.
   * @param columnTypes Hive types of the columns.
   */
  public HiveTableDescriptor( String tableName, String[] columnNames, String[] columnTypes )
    {
    this( HIVE_DEFAULT_DATABASE_NAME, tableName, columnNames, columnTypes, new String[]{}, HIVE_DEFAULT_DELIMITER,
      HIVE_DEFAULT_SERIALIZATION_LIB_NAME, null );
    }

  /**
   * Constructs a new HiveTableDescriptor object.
   *
   * @param tableName   The table name.
   * @param columnNames Names of the columns.
   * @param columnTypes Hive types of the columns.
   * @param partitionKeys The keys for partitioning the table.
   */
  public HiveTableDescriptor( String tableName, String[] columnNames, String[] columnTypes, String[] partitionKeys )
    {
    this( HIVE_DEFAULT_DATABASE_NAME, tableName, columnNames, columnTypes, partitionKeys, HIVE_DEFAULT_DELIMITER,
      HIVE_DEFAULT_SERIALIZATION_LIB_NAME, null );
    }

  /**
   * Constructs a new HiveTableDescriptor object.
   *
   * @param tableName   The table name.
   * @param columnNames Names of the columns.
   * @param columnTypes Hive types of the columns.
   * @param partitionKeys The keys for partitioning the table.
   * @param delimiter   The field delimiter of the Hive table.
   *
   */
  public HiveTableDescriptor( String tableName, String[] columnNames, String[] columnTypes, String[] partitionKeys, String delimiter )
    {
    this( HIVE_DEFAULT_DATABASE_NAME, tableName, columnNames, columnTypes, partitionKeys, delimiter,
      HIVE_DEFAULT_SERIALIZATION_LIB_NAME, null );
    }


  /**
   * Constructs a new HiveTableDescriptor object.
   *
   * @param databaseName The database name.
   * @param tableName   The table name.
   * @param columnNames Names of the columns.
   * @param columnTypes Hive types of the columns.
   */
  public HiveTableDescriptor( String databaseName, String tableName, String[] columnNames, String[] columnTypes )
    {
    this( databaseName, tableName, columnNames, columnTypes, new String[]{}, HIVE_DEFAULT_DELIMITER,
      HIVE_DEFAULT_SERIALIZATION_LIB_NAME, null );
    }

  /**
   * Constructs a new HiveTableDescriptor object.
   *
   * @param databaseName The database name.
   * @param tableName   The table name.
   * @param columnNames Names of the columns.
   * @param columnTypes Hive types of the columns.
   * @param partitionKeys The keys for partitioning the table.
   */
  public HiveTableDescriptor( String databaseName, String tableName, String[] columnNames, String[] columnTypes, String[] partitionKeys )
    {
    this( databaseName, tableName, columnNames, columnTypes, partitionKeys, HIVE_DEFAULT_DELIMITER,
      HIVE_DEFAULT_SERIALIZATION_LIB_NAME, null );
    }



  /**
   * Constructs a new HiveTableDescriptor object.
   *
   * @param databaseName     The database name.
   * @param tableName   The table name.
   * @param columnNames Names of the columns.
   * @param columnTypes Hive types of the columns.
   * @param partitionKeys The keys for partitioning the table.
   * @param delimiter   The field delimiter of the Hive table.
   *
   */
  public HiveTableDescriptor( String databaseName, String tableName, String[] columnNames, String[] columnTypes,
                              String[] partitionKeys, String delimiter )
    {
    this( databaseName, tableName, columnNames, columnTypes, partitionKeys, delimiter,
      HIVE_DEFAULT_SERIALIZATION_LIB_NAME, null );
    }

  /**
   * Constructs a new HiveTableDescriptor object.
   *
   * @param databaseName     The database name.
   * @param tableName        The table name
   * @param columnNames      Names of the columns
   * @param columnTypes      Hive types of the columns
   * @param delimiter        The field delimiter of the Hive table
   * @param serializationLib Hive serialization library.
   */
  public HiveTableDescriptor( String databaseName, String tableName, String[] columnNames, String[] columnTypes,
                              String[] partitionKeys, String delimiter,
                              String serializationLib, Path location )
    {
    if( tableName == null || tableName.isEmpty() )
      throw new IllegalArgumentException( "tableName cannot be null or empty" );
    if ( databaseName == null || tableName.isEmpty() )
      this.databaseName = HIVE_DEFAULT_DATABASE_NAME;
    else
      this.databaseName = databaseName.toLowerCase();
    this.tableName = tableName.toLowerCase();
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.partitionKeys = partitionKeys;
    this.serializationLib = serializationLib;
    //Only set the delimiter if the serialization lib is Delimited.
    if( delimiter == null && this.serializationLib == HIVE_DEFAULT_SERIALIZATION_LIB_NAME )
      this.delimiter = HIVE_DEFAULT_DELIMITER;
    else
      this.delimiter = delimiter;
    if ( isPartitioned() )
      verifyPartitionKeys();
    if( columnNames.length == 0 || columnTypes.length == 0 || columnNames.length != columnTypes.length )
      throw new IllegalArgumentException( "columnNames and columnTypes cannot be empty and must have the same size" );

    if( location != null )
      {
      if( !location.isAbsolute() )
        throw new IllegalArgumentException( "location must be a fully qualified absolute path" );

      // Store as string since path is not serialisable
      this.location = location.toString();
      }
    }

  /**
   * Private method to verify that all partition keys are also listed as column keys.
   */
  private void verifyPartitionKeys()
    {
    for( int index = 0; index < partitionKeys.length; index++ )
      {
      String key = partitionKeys[ index ];
      if( !caseInsensitiveContains( columnNames, key ) )
        throw new IllegalArgumentException( String.format( "Given partition key '%s' not present in column names", key ) );
      }
    }

  /**
   * Converts the instance to a Hive Table object, which can be used with the MetaStore API.
   *
   * @return a new Table instance.
   */
  public Table toHiveTable()
    {
    Table table = new Table();
    table.setDbName( getDatabaseName() );
    table.setTableName( tableName );
    table.setTableType( TableType.MANAGED_TABLE.toString() );

    StorageDescriptor sd = new StorageDescriptor();
    for( int index = 0; index < columnNames.length; index++ )
      {
      String columnName = columnNames[ index ];
      if ( !caseInsensitiveContains(partitionKeys, columnName ) )
        // calling toLowerCase() on the type to match the behaviour of the hive console
        sd.addToCols( new FieldSchema( columnName, columnTypes[ index ].toLowerCase(), "created by Cascading" ) );
      }
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib( serializationLib );
    Map<String, String> serDeParameters = new HashMap<String, String>();

    if ( getDelimiter() != null)
      {
      serDeParameters.put( "serialization.format", getDelimiter() );
      serDeParameters.put( "field.delim", getDelimiter() );
      }
    else
      {
      serDeParameters.put( "serialization.format", "1" );
      }
    serDeInfo.setParameters( serDeParameters );

    sd.setSerdeInfo( serDeInfo );
    sd.setInputFormat( HIVE_DEFAULT_INPUT_FORMAT_NAME );
    sd.setOutputFormat( HIVE_DEFAULT_OUTPUT_FORMAT_NAME );

    if ( location != null )
      {
      table.setTableType( TableType.EXTERNAL_TABLE.toString() );
      // Need to set this as well since setting the table type would be too obvious
      table.putToParameters( "EXTERNAL", "TRUE" );
      sd.setLocation( location.toString() );
      }

    table.setSd( sd );

    if ( isPartitioned() )
      {
      table.setPartitionKeys( createPartitionSchema() );
      table.setPartitionKeysIsSet( true );
      }

    return table;
    }

  /**
   * Creates a List of FieldSchema instances representing the partitions of the Hive Table.
   * @return a List of FieldSchema instances.
   */
  private List<FieldSchema> createPartitionSchema()
    {
    List<String> names = Arrays.asList( columnNames );
    List<FieldSchema> schema = new LinkedList<FieldSchema>();
    for( int i = 0; i < partitionKeys.length; i++ )
      {
      int index = names.indexOf( partitionKeys[ i ] );
      schema.add( new FieldSchema( columnNames[ index ], columnTypes[ index ], "" ) );
      }
    return schema;
    }

  /**
   * Returns a new Partition object to be used with a HivePartitionTap. If the table is not partitioned the method
   * will return null.
   * @return a new partition object or null.
   */
   public Partition getPartition()
     {
     if ( isPartitioned() )
       return new HivePartition( new Fields( getPartitionKeys() ) );
     throw new CascadingException( "non partitioned table cannot be used in a partitioned context" );
     }


  /**
   * Converts the HiveTableDescriptor to a Fields instance. If the table is partitioned only the columns not
   * part of the partitioning will be returned.
   * @return A Fields instance.
   */
   public Fields toFields()
    {
    if ( !isPartitioned() )
      return new Fields( columnNames );

    List<String> names =  new ArrayList<String>( Arrays.asList( columnNames ) );
    names.removeAll( Arrays.asList( getPartitionKeys() ) );

    Comparable [] comparables = new Comparable[names.size()];
    return new Fields( (Comparable[]) names.toArray( comparables ) );
    }


  /**
   * Returns the path of the table within the warehouse directory.
   * @return The path of the table within the warehouse directory.
   */
  public String getLocation( String warehousePath )
    {
    if (location != null)
      return location.toString();
    else if ( getDatabaseName().equals( HIVE_DEFAULT_DATABASE_NAME ) )
      return String.format( "%s/%s", warehousePath, getTableName() );
    else
      return String.format( "%s/%s.db/%s", warehousePath, getDatabaseName(), getTableName() );
    }

  /**
   * Converts the HiveTableDescriptor to a Scheme instance based on the information available.
   *
   * @return a new Scheme instance.
   */
  public Scheme toScheme()
    {
    // TODO add smarts to return the right thing.
    Scheme scheme = new TextDelimited( false, getDelimiter() );
    scheme.setSinkFields( toFields() );
    return scheme;
    }

  public String[] getColumnNames()
    {
    return columnNames;
    }

  public String[] getColumnTypes()
    {
    return columnTypes;
    }

  public String getTableName()
    {
    return tableName;
    }

  public String getDatabaseName()
    {
    return databaseName;
    }

  public String getDelimiter()
    {
    return delimiter;
    }

  public String[] getPartitionKeys()
    {
    return partitionKeys;
    }

  public boolean isPartitioned()
    {
    return partitionKeys != null && partitionKeys.length > 0;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    HiveTableDescriptor that = (HiveTableDescriptor) object;

    if( !arraysEqualCaseInsensitive( columnNames, that.columnNames ) )
      return false;
    if( !arraysEqualCaseInsensitive( columnTypes, that.columnTypes ) )
      return false;
    if( databaseName != null ? !databaseName.equalsIgnoreCase( that.databaseName ) : that.databaseName != null )
      return false;
    if( delimiter != null ? !delimiter.equals( that.delimiter ) : that.delimiter != null )
      return false;
    if( !arraysEqualCaseInsensitive( partitionKeys, that.partitionKeys ) )
      return false;
    if( serializationLib != null ? !serializationLib.equals( that.serializationLib ) : that.serializationLib != null )
      return false;
    if( tableName != null ? !tableName.equalsIgnoreCase( that.tableName ) : that.tableName != null )
      return false;
    if( location != null ? !location.equals( that.location ) : that.location != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = partitionKeys != null ? arraysHashCodeCaseInsensitive( partitionKeys ) : 0;
    result = 31 * result + ( delimiter != null ? delimiter.hashCode() : 0 );
    result = 31 * result + ( tableName != null ? tableName.toLowerCase().hashCode() : 0 );
    result = 31 * result + ( databaseName != null ? databaseName.toLowerCase().hashCode() : 0 );
    result = 31 * result + ( columnNames != null ? arraysHashCodeCaseInsensitive( columnNames ) : 0 );
    result = 31 * result + ( columnTypes != null ? arraysHashCodeCaseInsensitive( columnTypes ) : 0 );
    result = 31 * result + ( serializationLib != null ? serializationLib.hashCode() : 0 );
    result = 31 * result + ( location != null ? location.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    return "HiveTableDescriptor{" +
      "partitionKeys=" + Arrays.toString( partitionKeys ) +
      ", delimiter='" + delimiter + '\'' +
      ", tableName='" + tableName + '\'' +
      ", databaseName='" + databaseName + '\'' +
      ", columnNames=" + Arrays.toString( columnNames ) +
      ", columnTypes=" + Arrays.toString( columnTypes ) +
      ", serializationLib='" + serializationLib + '\'' +
      ( location != null ? ", location='" + location + '\'' : "" ) +
      '}';
    }

  private static boolean arraysEqualCaseInsensitive( String[] left, String[] right )
    {
    if( left.length != right.length )
      return false;

    for( int index = 0; index < left.length; index++ )
      if( !left[ index ].equalsIgnoreCase( right[ index ] ) )
        return false;

    return true;
    }

  private boolean caseInsensitiveContains( String[] data, String key)
    {
    boolean found = false;
    for( int i = 0; i < data.length && !found; i++ )
      {
      if( data[i].equalsIgnoreCase( key ) )
        found = true;
      }
    return found;
    }

  private static int arraysHashCodeCaseInsensitive( String[] strings )
    {
    String[] lower = new String[ strings.length ];
    for ( int index = 0; index < strings.length; index++ )
      lower[ index ] = strings[ index ].toLowerCase();

    return Arrays.hashCode( lower );
    }
  }
