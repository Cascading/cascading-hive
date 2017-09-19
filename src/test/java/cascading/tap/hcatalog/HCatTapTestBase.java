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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.LinkedList;
import java.util.List;

import cascading.HiveTestCase;
import cascading.flow.Flow;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hcatalog.HCatScheme;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public abstract class HCatTapTestBase extends HiveTestCase
  {
  private static final long serialVersionUID = 1L;

  private static final String DATABASE_NAME = "my_db";
  private static final String PARTITIONED_TABLE_NAME = "my_partitioned_table";
  private static final String UNPARTITIONED_TABLE_NAME = "my_unpartitioned_table";
  private static final String NEW_TABLE_NAME = "my_new_table";

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public final String tableFileFormat;

  private File fileFolder;
  private File dbFolder;
  private File partitionedTableFolder;
  private File unpartitionedTableFolder;

  private Fields dataFields;

  private Fields partitionFields;

  HCatTapTestBase( String tableFileFormat )
    {
    this.tableFileFormat = tableFileFormat;
    }

  @Before
  public void init() throws IOException
    {
    fileFolder = temp.newFolder( "file" );
    dbFolder = temp.newFolder( DATABASE_NAME );

    runHiveQuery( String.format( "CREATE DATABASE %s LOCATION '%s'", DATABASE_NAME, dbFolder.getCanonicalPath() ) );

    dataFields = new Fields( new String[] { "foo", "bar" }, new Type[] { String.class, Integer.class } );
    partitionFields = new Fields( "baz", String.class );
    }

  private void createPartitionedTable() throws IOException
    {
    partitionedTableFolder = temp.newFolder( PARTITIONED_TABLE_NAME );
    runHiveQuery( String.format(
        "CREATE TABLE %s.%s (foo STRING, bar INT) PARTITIONED BY (baz STRING) STORED AS %s LOCATION '%s'",
        DATABASE_NAME, PARTITIONED_TABLE_NAME, tableFileFormat, partitionedTableFolder.getCanonicalPath() ) );
    }

  private void createUnpartitionedTable() throws IOException
    {
    unpartitionedTableFolder = temp.newFolder( UNPARTITIONED_TABLE_NAME );
    runHiveQuery( String.format( "CREATE TABLE %s.%s (foo STRING, bar INT) STORED AS %s LOCATION '%s'", DATABASE_NAME,
        UNPARTITIONED_TABLE_NAME, tableFileFormat, unpartitionedTableFolder.getCanonicalPath() ) );
    }

  @After
  public void cleanup()
    {
    runHiveQuery( String.format( "DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, PARTITIONED_TABLE_NAME ) );
    runHiveQuery( String.format( "DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, UNPARTITIONED_TABLE_NAME ) );
    runHiveQuery( String.format( "DROP DATABASE IF EXISTS %s", DATABASE_NAME ) );
    }

  @Test
  public void fromPartitionedTableToFile() throws Exception
    {
    createPartitionedTable();

    runHiveQuery( String.format( "INSERT INTO TABLE %s.%s PARTITION (baz='x') VALUES ('a','1')", DATABASE_NAME,
        PARTITIONED_TABLE_NAME ) );
    runHiveQuery( String.format( "INSERT INTO TABLE %s.%s PARTITION (baz='y') VALUES ('b','2')", DATABASE_NAME,
        PARTITIONED_TABLE_NAME ) );

    HCatTap source = new HCatTap( new HCatScheme( Fields.join( dataFields, partitionFields ) ), DATABASE_NAME,
        PARTITIONED_TABLE_NAME );

    Fields fields = dataFields.append( partitionFields );
    File output = new File( fileFolder, "data" );
    Hfs sink = new Hfs( new TextDelimited( fields, "\t" ), output.getCanonicalPath() );

    Pipe pipe = new Pipe( "pipe" );
    Flow<?> flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );
    flow.complete();

    List<String> lines = readLines( new Path( output.getCanonicalPath() ) );
    assertEquals( 2, lines.size() );
    assertTrue( "Expecting line 'a\t1\tx' but not found", lines.contains( "a\t1\tx" ) );
    assertTrue( "Expecting line 'b\t2\ty' but not found", lines.contains( "b\t2\ty" ) );
    }

  @Test
  public void fromFileToPartitionedTable() throws Exception
    {
    createPartitionedTable();

    try ( Writer writer = new FileWriter( new File( fileFolder, "data" ) ) )
      {
      writer.write( "a\t1\tx\nb\t2\ty\n" );
      }

    Fields fields = dataFields.append( partitionFields );
    Hfs source = new Hfs( new TextDelimited( fields, "\t" ), fileFolder.getCanonicalPath() );

    HCatTap sink = new HCatTap( new HCatScheme( Fields.join( dataFields, partitionFields ) ), DATABASE_NAME,
        PARTITIONED_TABLE_NAME );

    Pipe pipe = new Pipe( "pipe" );
    Flow<?> flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );
    flow.complete();

    List<Partition> listPartitions = createMetaStoreClient().listPartitions( DATABASE_NAME, PARTITIONED_TABLE_NAME,
        (short) -1 );
    assertEquals( 2, listPartitions.size() );
    assertEquals( "x", listPartitions.get( 0 ).getValues().get( 0 ) );
    assertEquals( "y", listPartitions.get( 1 ).getValues().get( 0 ) );

    List<Object> rows = runHiveQuery(
        String.format( "SELECT * FROM %s.%s ORDER BY bar", DATABASE_NAME, PARTITIONED_TABLE_NAME ) );
    assertEquals( 2, rows.size() );
    assertEquals( "a\t1\tx", rows.get( 0 ) );
    assertEquals( "b\t2\ty", rows.get( 1 ) );
    }

  @Test
  public void fromUnpartitionedTableToFile() throws Exception
    {
    createUnpartitionedTable();

    runHiveQuery(
        String.format( "INSERT INTO TABLE %s.%s VALUES ('a','1'), ('b','2')", DATABASE_NAME, UNPARTITIONED_TABLE_NAME ) );

    HCatTap source = new HCatTap( new HCatScheme( dataFields ), DATABASE_NAME, UNPARTITIONED_TABLE_NAME );

    File output = new File( fileFolder, "data" );
    Hfs sink = new Hfs( new TextDelimited( dataFields, "\t" ), output.getCanonicalPath() );

    Pipe pipe = new Pipe( "pipe" );
    Flow<?> flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );
    flow.complete();

    List<String> lines = readLines( new Path( output.getCanonicalPath() ) );
    assertEquals( 2, lines.size() );
    assertTrue( "Expecting line 'a\t1' but not found", lines.contains( "a\t1" ) );
    assertTrue( "Expecting line 'b\t2' but not found", lines.contains( "b\t2" ) );
    }

  @Test
  public void fromFileToUnpartitionedTable() throws Exception
    {
    createUnpartitionedTable();

    try ( Writer writer = new FileWriter( new File( fileFolder, "data" ) ) )
      {
      writer.write( "a\t1\nb\t2\n" );
      }

    Hfs source = new Hfs( new TextDelimited( dataFields, "\t" ), fileFolder.getCanonicalPath() );

    HCatTap sink = new HCatTap( new HCatScheme( dataFields ), DATABASE_NAME, UNPARTITIONED_TABLE_NAME );

    Pipe pipe = new Pipe( "pipe" );
    Flow<?> flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );
    flow.complete();

    List<Object> rows = runHiveQuery(
        String.format( "SELECT * FROM %s.%s ORDER BY bar", DATABASE_NAME, UNPARTITIONED_TABLE_NAME ) );
    assertEquals( 2, rows.size() );
    assertEquals( "a\t1", rows.get( 0 ) );
    assertEquals( "b\t2", rows.get( 1 ) );
    }

  @Test
  public void fromPartitionedTableToUnpartitionedTable() throws Exception
    {
    createPartitionedTable();
    createUnpartitionedTable();

    runHiveQuery( String.format( "INSERT INTO TABLE %s.%s PARTITION (baz='x') VALUES ('a','1')", DATABASE_NAME,
        PARTITIONED_TABLE_NAME ) );
    runHiveQuery( String.format( "INSERT INTO TABLE %s.%s PARTITION (baz='y') VALUES ('b','2')", DATABASE_NAME,
        PARTITIONED_TABLE_NAME ) );

    HCatTap source = new HCatTap( new HCatScheme( Fields.join( dataFields, partitionFields ) ), DATABASE_NAME,
        PARTITIONED_TABLE_NAME );

    HCatTap sink = new HCatTap( new HCatScheme( dataFields ), DATABASE_NAME, UNPARTITIONED_TABLE_NAME );

    Pipe pipe = new Pipe( "pipe" );
    Flow<?> flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );
    flow.complete();

    List<Object> rows = runHiveQuery(
        String.format( "SELECT * FROM %s.%s ORDER BY bar", DATABASE_NAME, UNPARTITIONED_TABLE_NAME ) );
    assertEquals( 2, rows.size() );
    assertEquals( "a\t1", rows.get( 0 ) );
    assertEquals( "b\t2", rows.get( 1 ) );
    }

  @Test
  public void fromUnpartitionedTableToPartitionedTable() throws Exception
    {
    createPartitionedTable();
    createUnpartitionedTable();

    runHiveQuery(
        String.format( "INSERT INTO TABLE %s.%s VALUES ('a','1'), ('b','2')", DATABASE_NAME, UNPARTITIONED_TABLE_NAME ) );

    HCatTap source = new HCatTap( new HCatScheme( dataFields ), DATABASE_NAME, UNPARTITIONED_TABLE_NAME );

    HCatTap sink = new HCatTap( new HCatScheme( Fields.join( dataFields, partitionFields ) ), DATABASE_NAME,
        PARTITIONED_TABLE_NAME );

    Pipe pipe = new Pipe( "pipe" );
    pipe = new Each( pipe, new Insert( partitionFields, "x" ), Fields.ALL );
    Flow<?> flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );
    flow.complete();

    List<Partition> listPartitions = createMetaStoreClient().listPartitions( DATABASE_NAME, PARTITIONED_TABLE_NAME,
        (short) -1 );
    assertEquals( 1, listPartitions.size() );
    assertEquals( "x", listPartitions.get( 0 ).getValues().get( 0 ) );

    List<Object> rows = runHiveQuery(
        String.format( "SELECT * FROM %s.%s ORDER BY bar", DATABASE_NAME, PARTITIONED_TABLE_NAME ) );
    assertEquals( 2, rows.size() );
    assertEquals( "a\t1\tx", rows.get( 0 ) );
    assertEquals( "b\t2\tx", rows.get( 1 ) );
    }

  @Test
  public void resourceExists() throws Exception
    {
    createPartitionedTable();
    HCatTap source = new HCatTap( new HCatScheme( Fields.join( dataFields, partitionFields ) ), DATABASE_NAME,
        PARTITIONED_TABLE_NAME );
    assertTrue( source.resourceExists( createHiveConf() ) );
    }

  @Test
  public void resourceDoesNotExist() throws Exception
    {
    HCatTap source = new HCatTap( new HCatScheme( Fields.join( dataFields, partitionFields ) ), DATABASE_NAME,
        NEW_TABLE_NAME );
    assertFalse( source.resourceExists( createHiveConf() ) );
    }

  @Test
  public void modifiedTime() throws Exception
    {
    createPartitionedTable();
    HCatTap source = new HCatTap( new HCatScheme( Fields.join( dataFields, partitionFields ) ), DATABASE_NAME,
        PARTITIONED_TABLE_NAME );
    assertTrue( source.getModifiedTime( createHiveConf() ) > 0 );
    }

  @Test
  public void readWithFilterFromPartitionedTable() throws Exception
    {
    createPartitionedTable();

    runHiveQuery( String.format( "INSERT INTO TABLE %s.%s PARTITION (baz='x') VALUES ('a','1')", DATABASE_NAME,
        PARTITIONED_TABLE_NAME ) );
    runHiveQuery( String.format( "INSERT INTO TABLE %s.%s PARTITION (baz='y') VALUES ('b','2')", DATABASE_NAME,
        PARTITIONED_TABLE_NAME ) );

    HCatTap source = new HCatTap( new HCatScheme( Fields.join( dataFields, partitionFields ) ), DATABASE_NAME,
        PARTITIONED_TABLE_NAME, "baz='y'" );

    Fields fields = dataFields.append( partitionFields );
    File output = new File( fileFolder, "data" );
    Hfs sink = new Hfs( new TextDelimited( fields, "\t" ), output.getCanonicalPath() );

    Pipe pipe = new Pipe( "pipe" );
    Flow<?> flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );
    flow.complete();

    List<String> lines = readLines( new Path( output.getCanonicalPath() ) );
    assertEquals( 1, lines.size() );
    assertTrue( "Expecting line 'b\t2\ty' but not found", lines.contains( "b\t2\ty" ) );
    }

  @Test
  public void readWithFilterFromUnpartitionedTable() throws Exception
    {
    createUnpartitionedTable();

    runHiveQuery(
        String.format( "INSERT INTO TABLE %s.%s VALUES ('a','1'), ('b','2')", DATABASE_NAME, UNPARTITIONED_TABLE_NAME ) );

    HCatTap source = new HCatTap( new HCatScheme( dataFields ), DATABASE_NAME, UNPARTITIONED_TABLE_NAME, "foo='b'" ); // Filter is ignored

    File output = new File( fileFolder, "data" );
    Hfs sink = new Hfs( new TextDelimited( dataFields, "\t" ), output.getCanonicalPath() );

    Pipe pipe = new Pipe( "pipe" );
    Flow<?> flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );
    flow.complete();

    List<String> lines = readLines( new Path( output.getCanonicalPath() ) );
    assertEquals( 2, lines.size() );
    assertTrue( "Expecting line 'a\t1' but not found", lines.contains( "a\t1" ) );
    assertTrue( "Expecting line 'b\t2' but not found", lines.contains( "b\t2" ) );
    }

  @Test
  public void columnProjectionPartitionedTable() throws Exception
    {
    createPartitionedTable();

    runHiveQuery( String.format( "INSERT INTO TABLE %s.%s PARTITION (baz='x') VALUES ('a','1')", DATABASE_NAME,
        PARTITIONED_TABLE_NAME ) );
    runHiveQuery( String.format( "INSERT INTO TABLE %s.%s PARTITION (baz='y') VALUES ('b','2')", DATABASE_NAME,
        PARTITIONED_TABLE_NAME ) );

    Fields projectedFields = new Fields( "bar", Integer.class );
    HCatTap source = new HCatTap( new HCatScheme( Fields.join( projectedFields, partitionFields ) ), DATABASE_NAME,
        PARTITIONED_TABLE_NAME, "baz='y'" );

    Fields fields = projectedFields.append( partitionFields );
    File output = new File( fileFolder, "data" );
    Hfs sink = new Hfs( new TextDelimited( fields, "\t" ), output.getCanonicalPath() );

    Pipe pipe = new Pipe( "pipe" );
    Flow<?> flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );
    flow.complete();

    List<String> lines = readLines( new Path( output.getCanonicalPath() ) );
    assertEquals( 1, lines.size() );
    assertTrue( "Expecting line '2\ty' but not found", lines.contains( "2\ty" ) );
    }

  @Test
  public void columnProjectionUnartitionedTable() throws Exception
    {
    createUnpartitionedTable();

    runHiveQuery(
        String.format( "INSERT INTO TABLE %s.%s VALUES ('a','1'), ('b','2')", DATABASE_NAME, UNPARTITIONED_TABLE_NAME ) );

    Fields projectedFields = new Fields( "bar", Integer.class );
    HCatTap source = new HCatTap( new HCatScheme( projectedFields ), DATABASE_NAME, UNPARTITIONED_TABLE_NAME );

    File output = new File( fileFolder, "data" );
    Hfs sink = new Hfs( new TextDelimited( projectedFields, "\t" ), output.getCanonicalPath() );

    Pipe pipe = new Pipe( "pipe" );
    Flow<?> flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );
    flow.complete();

    List<String> lines = readLines( new Path( output.getCanonicalPath() ) );
    assertEquals( 2, lines.size() );
    assertTrue( "Expecting line '1' but not found", lines.contains( "1" ) );
    assertTrue( "Expecting line '2' but not found", lines.contains( "2" ) );
    }

  private static List<String> readLines( Path path ) throws Exception
    {
    List<String> lines = new LinkedList<>();
    LocalFileSystem fs = FileSystem.getLocal( new Configuration() );
    FileStatus[] statuses = fs.listStatus( path );
    for ( FileStatus status : statuses )
      {
      BufferedReader br = new BufferedReader( new InputStreamReader( fs.open( status.getPath() ) ) );
      String line = null;
      while ( ( line = br.readLine() ) != null )
        {
        lines.add( line );
        }
      }
    return lines;
    }

  }
