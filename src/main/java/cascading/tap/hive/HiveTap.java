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
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HiveTap is Tap implementation, which can create Hive tables on HDFS. HiveTap supports a strict mode, which will
 * make sure that an existing table has the same structure as the one requested by the user. This behaviour is off by
 * default and can be enabled by passing strict=true to the constructor.
 */
public class HiveTap extends Hfs
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HiveTap.class );

  /** TableDescriptor for the table. */
  private final HiveTableDescriptor tableDescriptor;

  /** HiveConf object */
  private transient HiveConf hiveConf;

  /** strict mode enforces that an existing table has to match the given TableDescriptor*/
  private boolean strict;

  /** last modified time */
  private long modifiedTime;

  /**
   * Constructs a new HiveTap instance.
   * @param hiveConf The HiveConf object to use.
   * @param tableDesc The HiveTableDescriptor for creating and validating Hive tables.
   * @param scheme   The Scheme to be used by the Tap.
   */
  public HiveTap( HiveConf hiveConf, HiveTableDescriptor tableDesc, Scheme scheme )
    {
    this( hiveConf, tableDesc, scheme, SinkMode.REPLACE, false );
    }
  /**
   * Constructs a new HiveTap instance.
   * @param hiveConf The HiveConf object to use.
   * @param tableDesc The HiveTableDescriptor for creating and validating Hive tables.
   * @param scheme   The Scheme to be used by the Tap.
   * @param mode The SinkMode to use
   * @param strict Enables and disables strict validation of hive tables.
   */
  public HiveTap( HiveConf hiveConf, HiveTableDescriptor tableDesc, Scheme scheme, SinkMode mode, boolean strict )
    {
    super(scheme, String.format("%s/%s", hiveConf.get( HiveConf.ConfVars.METASTOREWAREHOUSE.varname ),
      tableDesc.getTableName() ), mode);
    setScheme( scheme );
    this.hiveConf = hiveConf;
    this.tableDescriptor = tableDesc;
    this.strict = strict;
    }

  @Override
  public boolean createResource( JobConf conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      {
      FileSystem fs = FileSystem.get( conf );
      if ( !fs.exists( getPath() ) )
        FileSystem.get( conf ).mkdirs( getPath() );

      IMetaStoreClient metaStoreClient = null;
      try
        {
        metaStoreClient = createMetaStoreClient();
        metaStoreClient.createTable( tableDescriptor.toHiveTable() );
        modifiedTime = System.currentTimeMillis();
        return true;
        }
      catch( MetaException exception )
        {
        throw new IOException( exception );
        }
      catch( TException exception )
        {
        throw new IOException( exception );
        }
    finally
        {
        if( metaStoreClient != null )
          metaStoreClient.close();
        }
      }
    return true;
    }


  @Override
  public boolean resourceExists( JobConf conf ) throws IOException
    {
    IMetaStoreClient metaStoreClient = null;
    try
      {
      metaStoreClient = createMetaStoreClient();
      Table table = metaStoreClient.getTable( HiveTableDescriptor.HIVE_DEFAULT_DATABASE_NAME,
                                               tableDescriptor.getTableName() );
      modifiedTime = table.getLastAccessTime();
      // check if the schema matches the table descriptor. If not, throw an exception.
      if( strict )
        {
        LOG.info( "strict mode: comparing hive table with table descriptor" );
        List<FieldSchema> schemaList = table.getSd().getCols();
        if( schemaList.size() != tableDescriptor.getColumnNames().length )
          throw new HiveTableValidationException( "table in MetaStore does not match expectations" );
        for( int index = 0; index < schemaList.size(); index++ )
          {
          FieldSchema schema = schemaList.get( index );
          String expectedColumnName = tableDescriptor.getColumnNames()[ index ];
          String expectedColumnType = tableDescriptor.getColumnTypes()[ index ];
          if( !schema.getName().equals( expectedColumnName ) )
            throw new HiveTableValidationException( String.format(
              "hive schema mismatch: expected column name '%s', but found '%s'", expectedColumnName, schema.getName() ) );
          if( !schema.getType().equals( expectedColumnType ) )
            throw new HiveTableValidationException( String.format(
              "hive schema mismatch: expected column type '%s', but found '%s'", expectedColumnType, schema.getType() ) );
          }
        }
      return true;
      }
    catch( MetaException exception )
      {
      throw new IOException( exception );
      }
    catch( NoSuchObjectException exception )
      {
      return false;
      }
    catch( TException exception )
      {
      throw new IOException( exception );
      }
    finally
      {
      if ( metaStoreClient != null )
        metaStoreClient.close();
      }
    }

  @Override
  public boolean deleteResource( JobConf conf ) throws IOException
    {
    // hive does not create the directories, when creating a table. This works around that "problem"
    FileSystem fs = FileSystem.get( conf );
    if ( fs.exists( getPath() ) )
      return super.deleteResource( conf );
    return true;
    }


  @Override
  public long getModifiedTime( JobConf conf ) throws IOException
    {
    return modifiedTime;
    }

  /***
   * Private helper method to create a IMetaStore client.
   * @return a new IMetaStoreClient
   * @throws MetaException in case the creation fails.
   */
  private IMetaStoreClient createMetaStoreClient() throws MetaException
    {
    // it is a bit unclear if it is safe to re-use these, so we create a new one every time,
    // to be sure
    if ( hiveConf == null )
      hiveConf = new HiveConf(  );
    return RetryingMetaStoreClient.getProxy( hiveConf ,
      new HiveMetaHookLoader()
      {
      @Override
      public HiveMetaHook getHook( Table tbl ) throws MetaException
        {
        return null;
        }
      }, HiveMetaStoreClient.class.getName() );
    }
  }
