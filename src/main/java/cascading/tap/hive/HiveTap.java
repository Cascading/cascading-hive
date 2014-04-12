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

import cascading.CascadingException;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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

  /** strict mode enforces that an existing table has to match the given TableDescriptor */
  private boolean strict;

  /** last modified time */
  private long modifiedTime;

  /**
   * Constructs a new HiveTap instance.
   * @param tableDesc The HiveTableDescriptor for creating and validating Hive tables.
   * @param scheme   The Scheme to be used by the Tap.
   */
  public HiveTap( HiveTableDescriptor tableDesc, Scheme scheme )
    {
    this( tableDesc, scheme, SinkMode.REPLACE, false );
    }

  /**
   * Constructs a new HiveTap instance.
   * @param tableDesc The HiveTableDescriptor for creating and validating Hive tables.
   * @param scheme   The Scheme to be used by the Tap.
   * @param mode The SinkMode to use
   * @param strict Enables and disables strict validation of hive tables.
   */
  public HiveTap( HiveTableDescriptor tableDesc, Scheme scheme, SinkMode mode, boolean strict )
    {
    super( scheme, null, mode );
    this.tableDescriptor = tableDesc;
    this.strict = strict;
    setScheme( scheme );

    setStringPath( String.format( "%s/%s", getHiveConf().get( HiveConf.ConfVars.METASTOREWAREHOUSE.varname ),
      tableDesc.getFilesystemPath() ) );

    }

  @Override
  public boolean createResource( JobConf conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      {
      Table hiveTable = tableDescriptor.toHiveTable();
        FileSystem fs = FileSystem.get( conf );
        if( !fs.exists( getPath() ) )
          FileSystem.get( conf ).mkdirs( getPath() );
      IMetaStoreClient metaStoreClient = null;
      try
        {
        metaStoreClient = createMetaStoreClient();
        try
          {
          metaStoreClient.getDatabase( tableDescriptor.getDatabaseName() );
          }
        // there is no databaseExists method in hive 0.10, so we have to use exceptions for flow control.
        catch( NoSuchObjectException exception )
          {
          LOG.info( "creating database '{}' at '{}' ", tableDescriptor.getDatabaseName(), getPath().getParent().toString() );
          Database db = new Database( tableDescriptor.getDatabaseName(), "created by Cascading",
                                      getPath().getParent().toString(), null );
          metaStoreClient.createDatabase( db );
          }
        LOG.info( "creating table '{}' at '{}' ", tableDescriptor.getTableName(), getPath().toString() );

        metaStoreClient.createTable( hiveTable );
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
      Table table = metaStoreClient.getTable( tableDescriptor.getDatabaseName(),
                                               tableDescriptor.getTableName() );
      modifiedTime = table.getLastAccessTime();
      // check if the schema matches the table descriptor. If not, throw an exception.
      if( strict )
        {
        LOG.info( "strict mode: comparing hive table with table descriptor" );
        if ( !table.getTableType().equals( tableDescriptor.toHiveTable().getTableType() ) )
          throw new HiveTableValidationException( String.format( "expected a table of type '%s' but found '%s'",
            tableDescriptor.toHiveTable().getTableType(), table.getTableType() ) );

        List<FieldSchema> schemaList = table.getSd().getCols();
        if( schemaList.size() != tableDescriptor.getColumnNames().length )
          throw new HiveTableValidationException( "table in MetaStore does not match expectations" );
        for( int index = 0; index < schemaList.size(); index++ )
          {
          FieldSchema schema = schemaList.get( index );
          String expectedColumnName = tableDescriptor.getColumnNames()[ index ];
          String expectedColumnType = tableDescriptor.getColumnTypes()[ index ];
          // this could be extended to the StorageDescriptor if necessary.
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
    if ( !resourceExists( conf ) )
      return true;
    IMetaStoreClient metaStoreClient = null;
    try
      {
      LOG.info( "dropping hive table {} in database {}", tableDescriptor.getTableName(), tableDescriptor.getDatabaseName() );
      metaStoreClient = createMetaStoreClient();
      metaStoreClient.dropTable( tableDescriptor.getDatabaseName(), tableDescriptor.getTableName(),
        true, true );
      }
    catch( MetaException exception )
      {
      throw new IOException( exception );
      }
    catch( NoSuchObjectException exception )
      {
      throw new IOException( exception );
      }
    catch( TException exception )
      {
      throw new IOException( exception );
      }
    finally
      {
      if (metaStoreClient != null )
        metaStoreClient.close();
      }
    return true;
    }


  /**
   * Registers a new Partition of a HiveTable. If the Partition already exists, it is ignored. If the current
   * table is not partitioned, the call is also ignored.
   *
   * @param conf  JobConf object of the current flow.
   * @param partition The partition to register.
   * @throws IOException In case any interaction with the HiveMetaStore fails
   */
  void registerPartition( JobConf conf, Partition partition ) throws IOException
    {
    if ( !tableDescriptor.isPartitioned() )
      return;

    if ( !resourceExists( conf ) )
      createResource( conf );

    IMetaStoreClient metaStoreClient = null;
    try
      {
      metaStoreClient = createMetaStoreClient();
      metaStoreClient.add_partition( partition );
      }
    catch( MetaException exception )
      {
      throw new IOException( exception );
      }
    catch( InvalidObjectException exception )
      {
      throw new IOException( exception );
      }
    catch( AlreadyExistsException exception )
      {
      // ignore
      }
    catch( TException exception )
      {
      throw new IOException( exception );
      }
    finally
      {
      if ( metaStoreClient != null );
        metaStoreClient.close();
      }
    }

  /**
   * Returns the current HiveConf object.
   *
   * @return the HiveConf object.
   * */
  private HiveConf getHiveConf()
    {
    if ( hiveConf == null )
      this.hiveConf = new HiveConf();
    return hiveConf;
    }

  HiveTableDescriptor getTableDescriptor()
    {
    return tableDescriptor;
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
    // it is a bit unclear if it is safe to re-use these instances, so we create a
    // new one every time, to be sure
    if ( hiveConf == null )
      hiveConf = new HiveConf();

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
