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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import cascading.CascadingException;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tuple.Fields;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * HiveTableDescriptor encapsulates information about a table in Hive like the table name, column names, type etc.
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

  /** default serialization lib name*/
  public static final String HIVE_DEFAULT_SERIALIZATION_LIB_NAME = HiveConf.ConfVars.HIVESCRIPTSERDE.defaultVal;

  /** field delimiter in the Hive table*/
  private String delimiter;

  /** name of the hive table */
  private String tableName;

  /** names of the columns */
  private String[] columnNames;

  /** hive column types */
  private String[] columnTypes;

  /** Hive serialization library */
  private String serializationLib;

  /**
   * Constructs a new HiveTableDescriptor object.
   *
   * @param tableName   The table name
   * @param columnNames Names of the columns
   * @param columnTypes Hive types of the columns
   */
  public HiveTableDescriptor( String tableName, String[] columnNames, String[] columnTypes )
    {
    this( tableName, columnNames, columnTypes, HIVE_DEFAULT_DELIMITER, HIVE_DEFAULT_SERIALIZATION_LIB_NAME );
    }

  /**
   * Constructs a new HiveTableDescriptor object.
   *
   * @param tableName   The table name.
   * @param columnNames Names of the columns.
   * @param columnTypes Hive types of the columns.
   * @param delimiter The field delimiter of the Hive table.
   */
  public HiveTableDescriptor( String tableName, String[] columnNames, String[] columnTypes, String delimiter )
    {
    this( tableName, columnNames, columnTypes, delimiter, HIVE_DEFAULT_SERIALIZATION_LIB_NAME );
    }


  /**
   * Constructs a new HiveTableDescriptor object.
   *
   * @param tableName   The table name
   * @param columnNames Names of the columns
   * @param columnTypes Hive types of the columns
   * @param delimiter The field delimiter of the Hive table
   * @param serializationLib Hive serialization library.
   */
  public HiveTableDescriptor( String tableName, String[] columnNames, String[] columnTypes, String delimiter,
                              String serializationLib )
    {
    if( tableName == null || tableName.isEmpty() )
      throw new CascadingException( "tableName cannot be null or empty" );
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.serializationLib = serializationLib;
    if ( delimiter == null )
      this.delimiter = HIVE_DEFAULT_DELIMITER;
    else
      this.delimiter = delimiter;
    if( columnNames.length == 0 || columnTypes.length == 0 || columnNames.length != columnTypes.length )
      throw new CascadingException( "columnNames and columnTypes cannot be empty and must have the same size" );
    }


  /**
   * Converts the instance to Hive Table, which can be used with the MetaStore API.
   *
   * @return a new HiveTable instance.
   */
  public Table toHiveTable()
    {
    Table table = new Table();
    table.setDbName( HIVE_DEFAULT_DATABASE_NAME );
    table.setTableName( tableName );

    StorageDescriptor sd = new StorageDescriptor();
    for( int index = 0; index < columnNames.length; index++ )
      sd.addToCols( new FieldSchema( columnNames[ index ], columnTypes[ index ], "" ) );

    // TODO inspecting the Scheme for this might make sense. We might move this method elsewhere
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib( serializationLib );
    Map<String, String> serDeParameters = new HashMap<String, String>(  );
    serDeParameters.put( "serialization.format", getDelimiter() );
    serDeParameters.put( "field.delim", getDelimiter() );

    sd.setSerdeInfo( serDeInfo );
    sd.setInputFormat( HIVE_DEFAULT_INPUT_FORMAT_NAME );
    sd.setOutputFormat( HIVE_DEFAULT_OUTPUT_FORMAT_NAME );
    table.setSd( sd );

    return table;
    }

  /**
   * Converts the HiveTableDescriptor to a Fields instance using the column names.
   * @return a Fields instance.
   */
  public Fields toFields()
    {
    // TODO if we need types, implement a mapping here.
    return new Fields( getColumnNames() );
    }

  /**
   * Converts the HiveTableDescriptor to a Scheme instance based on the information available.
   *
   * @return a new Scheme instance.
   */
  public Scheme toScheme()
    {
    // TODO add smarts to return the right thing.
    return new TextDelimited( false, getDelimiter() );
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

  public String getDelimiter()
    {
    return delimiter;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      {
      return true;
      }
    if( object == null || getClass() != object.getClass() )
      {
      return false;
      }

    HiveTableDescriptor that = (HiveTableDescriptor) object;

    if( !Arrays.equals( columnNames, that.columnNames ) )
      {
      return false;
      }
    if( !Arrays.equals( columnTypes, that.columnTypes ) )
      {
      return false;
      }
    if( !delimiter.equals( that.delimiter ) )
      {
      return false;
      }
    if( serializationLib != null ? !serializationLib.equals( that.serializationLib ) : that.serializationLib != null )
      {
      return false;
      }
    if( !tableName.equals( that.tableName ) )
      {
      return false;
      }

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = delimiter.hashCode();
    result = 31 * result + tableName.hashCode();
    result = 31 * result + ( columnNames != null ? Arrays.hashCode( columnNames ) : 0 );
    result = 31 * result + ( columnTypes != null ? Arrays.hashCode( columnTypes ) : 0 );
    result = 31 * result + ( serializationLib != null ? serializationLib.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    return "HiveTableDescriptor{" +
      "serializationLib='" + serializationLib + '\'' +
      ", columnTypes=" + Arrays.toString( columnTypes ) +
      ", columnNames=" + Arrays.toString( columnNames ) +
      ", tableName='" + tableName + '\'' +
      ", delimiter='" + delimiter + '\'' +
      '}';
    }
  }
