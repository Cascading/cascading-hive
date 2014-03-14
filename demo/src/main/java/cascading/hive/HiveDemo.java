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

package cascading.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hive.HiveFlow;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
/**
 *  Demo Application using HiveFlows which creates three tables, loads data, converts them in cascading and reads it back
 *  via JDBC.
 */
public class HiveDemo
  {

  private static String dualTableInputFile = System.getProperty( "user.dir" ) + "/src/main/resources/data.txt";

  /**
   */
  public static void main( String [] args ) throws Exception
    {

    Properties properties = new Properties();
    AppProps.setApplicationName( properties, "cascading hive integration demo" );

    // if you want to use a remote meta store, do this
    //new HiveProps().setMetastoreUris( "thrift://<metatorehost>:9083" ).addPropertiesTo( properties );

    // first we create a table called dual (like the one in oracle)
    HiveFlow createFlow = new HiveFlow( "create table dual", properties, "create table dual( value string )" );

    // create table flows have to run before other flows can interact with it, since the information has to be
    CascadeConnector connector = new CascadeConnector();
    connector.connect( createFlow ).complete();

    // create a second table with two columns
    HiveFlow createSecondTableFlow = new HiveFlow( "create table keyvalue", properties,
      " create table keyvalue (key string, value string) row format delimited fields terminated by '\\t' " );

    connector.connect( createSecondTableFlow ).complete();

    // load data from local fs into the hive table
    HiveFlow loadDataFlow = new HiveFlow( "load data into dual", properties,
      String.format( "load data local inpath '%s' overwrite into table dual",
        dualTableInputFile ) );

    // create a third table, with the same format as the second one
    HiveFlow createThirdTableFlow = new HiveFlow( "create table keyvalue2 ", properties,
      " create table keyvalue2 (key string, value string) row format delimited fields terminated by '\\t' " );

    // select data from dual and write into the second table
    HiveFlow selectFlow = new HiveFlow( "select data from dual into keyvalue", properties,
      "insert into table keyvalue select 'Hello' as key, 'hive!' as value from dual " );

    // simple function we use in a pure cascading flow
    class Upper extends BaseOperation implements Function
      {
      public Upper ( Fields fieldDeclaration )
        {
        super( 2, fieldDeclaration);
        }
      @Override
      public void operate( FlowProcess flowProcess, FunctionCall functionCall )
        {
        TupleEntry argument = functionCall.getArguments();
        String key = argument.getString( 0 ).toUpperCase();
        String value = argument.getString( 1 ).toUpperCase();
        Tuple result = new Tuple(key, value);
        functionCall.getOutputCollector().add( result );
        }
      }
    // create a pure cascading flow, that reads from the second table, converts everything to upper case and writes it into the third table
    Tap inTap = createSecondTableFlow.getSink();
    Tap outTap =  createThirdTableFlow.getSink();

    Pipe copyPipe = new Each( "convert keavalue to uppercase and write to keyvalue2", inTap.getSinkFields(), new Upper( inTap.getSinkFields() ), Fields.RESULTS ) ;
    Flow<?> pureCascadingFlow = new HadoopFlowConnector( ).connect( inTap, outTap, copyPipe );

    // run all of the above flows, cascading will figure out the right order of execution
    Cascade cascade = connector.connect( createThirdTableFlow, pureCascadingFlow, loadDataFlow, selectFlow );
    //cascade.writeDOT( "test.dot" );
    cascade.complete();

    // finally read the data from the table, that was filled by cascading via the Hive JDBC driver
    Class.forName( "org.apache.hive.jdbc.HiveDriver" );

    // change the url, if you use hiveserver2
    Connection con = DriverManager.getConnection( "jdbc:hive2://", "", "" );
    Statement stmt = con.createStatement();

    ResultSet rs = stmt.executeQuery( "select key, value from keyvalue2" );
    System.out.println("----------------------Hive JDBC--------------------------");
    while ( rs.next() )
      System.out.printf( "data from hive table copy: key=%s,value=%s\n", rs.getString( 1 ), rs.getString( 2 ) );
    System.out.println("---------------------------------------------------------");
    stmt.close();
    con.close();
    }

  }
