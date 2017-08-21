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

package cascading.tap.hcatalog;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.hadoop.mapred.InputFormatWrapper;
import cascading.hadoop.mapred.OutputFormatWrapper;
import cascading.scheme.hcatalog.HCatScheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.api.ObjectNotFoundException;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A HCatalog-backed {@link Tap} that represents a Hive table and that can be used as both source and sink.
 * <p>
 * When used as a source {@link Tap} it allows the use of features like partition pruning and column projection (if the
 * underlying file format backing the Hive tables support this).
 * </p>
 * <p>
 * When used as a sink {@link Tap} it uses
 * <a href="https://cwiki.apache.org/confluence/display/Hive/DynamicPartitions">Dynamic Partitions</a> to store the data
 * in the correct partition paths. Partitions are determine at runtime using the {@link Fields} representation of the
 * partition columns in {@link HCatScheme}. For example if a table is defined as:<br/>
 * <code>
 * CREATE TABLE mytable {
 * c INT
 * }
 * PARTITIONED BY (p STRING)
 * </code><br/>
 * column {@code p} must be provided with {@link HCatScheme} therefore when the {@linkplain Tuple tuples}
 * {@code (c=1,p='A')} and {@code (c=2,p='B')} are collected they will go to partitions {@code p='A'} and {@code p='B'}
 * respectively.
 * </p>
 * <p>
 * Note this {@link Tap} assumes that the Hive table exists.
 * </p>
 *
 * @since 2.1
 */
public class HCatTap extends Tap<Configuration, RecordReader, OutputCollector>
  {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger( HCatTap.class );

  private final String databaseName;
  private final String tableName;
  private final String filter;

  /**
   * Constructs a new {@link HCatTap} with no partition filter.
   * <p>
   * This is a convenient constructor for sink {@linkplain Tap Taps} where a partition filter is not required and
   * ignored.
   * </p>
   * <p>
   * This is equivalent to: <br/>
   * {@code new HCatTap(scheme, databaseName, tableName, null);}
   * </p>
   *
   * @param scheme       {@link HCatScheme} that describes the columns to be used.
   * @param databaseName Name of the Hive database where {@code tableName} is defined.
   * @param tableName    Name of the Hive table that this {@link Tap} represents.
   */
  public HCatTap( HCatScheme scheme, String databaseName, String tableName )
    {
    this( scheme, databaseName, tableName, null );
    }

  /**
   * Constructs a new {@link HCatTap}.
   * <p>
   * This constructor should be use by source {@linkplain Tap Taps} when partition filtering is required. See <a href=
   * "https://cwiki.apache.org/confluence/display/Hive/Hbase+execution+plans+for+RawStore+partition+filter+condition">
   * HCatalog filter expressions</a> for details about the {@code filter} format.
   * </p>
   *
   * @param scheme       {@link HCatScheme} that describes the columns to be used.
   * @param databaseName Name of the Hive database where {@code tableName} is defined.
   * @param tableName    Name of the Hive table that this {@link Tap} represents.
   * @param filter       A partition-selector expression.
   */
  public HCatTap( HCatScheme scheme, String databaseName, String tableName, String filter )
    {
    super( scheme, SinkMode.KEEP ); // KEEP prevents Cascading from calling deleteResource()
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.filter = filter;
    }

  @Override
  public String getIdentifier()
    {
    return String.format( "hcatalog://%s.%s-%s", databaseName, tableName, id(this));
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Configuration> flowProcess, Configuration conf )
    {
    InputFormatWrapper.setInputFormat( conf, HCatInputFormat.class, HCatInputFormatValueCopier.class );
    try
      {
      HCatInputFormat.setInput( conf, databaseName, tableName, filter );
      }
    catch( IOException e )
      {
      throw new RuntimeException( e );
      }
    super.sourceConfInit( flowProcess, conf );
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Configuration> flowProcess, RecordReader input )
    throws IOException
    {
    return new HadoopTupleEntrySchemeIterator( flowProcess, this, input );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Configuration> flowProcess, Configuration conf )
    {
    OutputFormatWrapper.setOutputFormat( conf, HCatOutputFormat.class );
    OutputJobInfo outputJobInfo = OutputJobInfo.create( databaseName, tableName, null );
    try
      {
      HCatOutputFormat.setOutput( conf, ( (JobConf) conf ).getCredentials(), outputJobInfo );
      }
    catch( IOException e )
      {
      throw new RuntimeException( e );
      }
    super.sinkConfInit( flowProcess, conf );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Configuration> flowProcess, OutputCollector output )
    throws IOException
    {
    return new HadoopTupleEntrySchemeCollector( flowProcess, this, output );
    }

  @Override
  public boolean resourceExists( Configuration conf ) throws IOException
    {
    HCatClient client = HCatClient.create( conf );
    try
      {
      client.getTable( databaseName, tableName );
      }
    catch( ObjectNotFoundException e )
      {
      return false;
      }
    finally
      {
      client.close();
      }
    return true;
    }

  @Override
  public boolean createResource( Configuration conf ) throws IOException
    {
    return false;
    }

  @Override
  public boolean deleteResource( Configuration conf ) throws IOException
    {
    return false;
    }

  @Override
  public long getModifiedTime( Configuration conf ) throws IOException
    {
    HCatClient client = HCatClient.create( conf );
    try
      {
      HCatTable table = client.getTable( databaseName, tableName );
      String lastModifiedTime = table.getTblProps().get( "last_modified_time" );
      if( lastModifiedTime == null )
        {
        return System.currentTimeMillis();
        }
      return Long.parseLong( lastModifiedTime );
      }
    finally
      {
      client.close();
      }
    }

  }
