package cascading.tap.hive;

import static cascading.tap.hive.HiveTableDescriptor.HIVE_ACID_TABLE_PARAMETER_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

import com.hotels.corc.cascading.OrcFile;

@RunWith(MockitoJUnitRunner.class)
public class AcidTapTest {

  private static final String[] COLUMN_NAMES = new String[] { "id", "msg" };
  private static final String[] COLUMN_TYPES = new String[] { "int", "string" };
  private static final String[] PARTITION_COLUMN_NAMES = new String[] { "id", "msg", "continent", "country" };
  private static final String[] PARTITION_COLUMN_TYPES = new String[] { "int", "string", "string", "string" };
  private static final String[] PARTITION_NAMES = new String[] { "continent", "country" };

  @Mock
  private MetaStoreClientFactory mockMetaStoreClientFactory;
  @Mock
  private IMetaStoreClient mockMetaStoreClient;
  @Mock
  private ValidTxnList mockValidTxnList;

  private File warehouseFile = new File("src/test/data");

  @Before
  public void mocks() throws Exception {
    // Logging only seems to work with this brute force approach.
    // PropertyConfigurator.configure(new File("src/test/resources/log4j.properties").getAbsolutePath());
    when(mockMetaStoreClientFactory.newInstance(any(Configuration.class))).thenReturn(mockMetaStoreClient);
    when(mockMetaStoreClient.getValidTxns()).thenReturn(mockValidTxnList);
  }

  @Test
  public void testReadAcidBaseWithDelta() throws Exception {
    Table table = createTable("acid_base_with_delta");

    when(mockMetaStoreClient.getTable(anyString(), anyString())).thenReturn(table);
    when(mockValidTxnList.toString()).thenReturn("7:");

    HiveTableDescriptor descriptor = createTableDescriptor(table, COLUMN_NAMES, COLUMN_TYPES);
    OrcFile scheme = OrcFile.source().declaredFields(descriptor.toFields()).schema(descriptor.toTypeInfo()).prependRowId().build();
    HiveTap tap = new HiveTap(descriptor, scheme, null, false, mockMetaStoreClientFactory);

    List<TupleEntry> tupleEntries = readTupleEntries(tap);
    assertThat(tupleEntries.size(), is(2));
    assertThat(tupleEntries.get(0).getTuple(), is(new Tuple(new RecordIdentifier(1, 0, 1), 2, "UPDATED: Streaming to welcome")));
    assertThat(tupleEntries.get(1).getTuple(), is(new Tuple(new RecordIdentifier(7, 0, 0), 3, "Testing Insert")));
  }

  @Test
  public void testReadAcidDeltaOnly() throws Exception {
    Table table = createTable("acid_delta_only");

    when(mockMetaStoreClient.getTable(anyString(), anyString())).thenReturn(table);
    when(mockValidTxnList.toString()).thenReturn("60:");

    HiveTableDescriptor descriptor = createTableDescriptor(table, COLUMN_NAMES, COLUMN_TYPES);
    OrcFile scheme = OrcFile.source().declaredFields(descriptor.toFields()).schema(descriptor.toTypeInfo()).prependRowId().build();
    HiveTap tap = new HiveTap(descriptor, scheme, null, false, mockMetaStoreClientFactory);

    List<TupleEntry> tupleEntries = readTupleEntries(tap);
    assertThat(tupleEntries.size(), is(3));
    assertThat(tupleEntries.get(0).getTuple(), is(new Tuple(new RecordIdentifier(60, 0, 0), 3, "z")));
    assertThat(tupleEntries.get(1).getTuple(), is(new Tuple(new RecordIdentifier(60, 0, 1), 2, "y")));
    assertThat(tupleEntries.get(2).getTuple(), is(new Tuple(new RecordIdentifier(60, 0, 2), 1, "x")));
  }

  @Test
  public void testReadPartitionedAcidBaseWithDelta() throws Exception {
    Table table = createTable("partition_acid_base_with_delta");

    when(mockMetaStoreClient.getTable(anyString(), anyString())).thenReturn(table);
    when(mockValidTxnList.toString()).thenReturn("7:");

    HiveTableDescriptor descriptor = createTableDescriptor(table, PARTITION_COLUMN_NAMES, PARTITION_COLUMN_TYPES,
        PARTITION_NAMES);
    OrcFile scheme = OrcFile.source().declaredFields(descriptor.toFields()).schema(descriptor.toTypeInfo()).prependRowId().build();
    HiveTap tap = new HiveTap(descriptor, scheme, null, false, mockMetaStoreClientFactory);
    HivePartitionTap partitionTap = new HivePartitionTap(tap);

    List<TupleEntry> tupleEntries = readTupleEntries(partitionTap);
    assertThat(tupleEntries.size(), is(2));
    assertThat(tupleEntries.get(0).getTuple(), is(new Tuple(new RecordIdentifier(1, 0, 1), 2, "UPDATED: Streaming to welcome", "Asia", "India")));
    assertThat(tupleEntries.get(1).getTuple(), is(new Tuple(new RecordIdentifier(7, 0, 0), 3, "Testing Insert", "Asia", "India")));
  }

  @Test
  public void testReadPartitionedAcidDeltaOnly() throws Exception {
    Table table = createTable("partition_acid_delta_only");

    when(mockMetaStoreClient.getTable(anyString(), anyString())).thenReturn(table);
    when(mockValidTxnList.toString()).thenReturn("60:");

    HiveTableDescriptor descriptor = createTableDescriptor(table, PARTITION_COLUMN_NAMES, PARTITION_COLUMN_TYPES,
        PARTITION_NAMES);
    OrcFile scheme = OrcFile.source().declaredFields(descriptor.toFields()).schema(descriptor.toTypeInfo()).prependRowId().build();
    HiveTap tap = new HiveTap(descriptor, scheme, null, false, mockMetaStoreClientFactory);
    HivePartitionTap partitionTap = new HivePartitionTap(tap);

    List<TupleEntry> tupleEntries = readTupleEntries(partitionTap);
    assertThat(tupleEntries.size(), is(3));
    assertThat(tupleEntries.get(0).getTuple(), is(new Tuple(new RecordIdentifier(60, 0, 0), 3, "z", "Asia", "India")));
    assertThat(tupleEntries.get(1).getTuple(), is(new Tuple(new RecordIdentifier(60, 0, 1), 2, "y", "Asia", "India")));
    assertThat(tupleEntries.get(2).getTuple(), is(new Tuple(new RecordIdentifier(60, 0, 2), 1, "x", "Asia", "India")));
  }

  private Table createTable(String tableName) {
    Table table = new Table();
    table.setTableName(tableName);

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(new File(warehouseFile, tableName).toURI().toString());
    table.setSd(sd);

    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put(HIVE_ACID_TABLE_PARAMETER_KEY, Boolean.TRUE.toString());
    table.setParameters(parameters);
    return table;
  }

  private HiveTableDescriptor createTableDescriptor(Table table, String[] columnNames, String[] columnTypes) {
    return createTableDescriptor(table, columnNames, columnTypes, new String[] {});
  }

  private HiveTableDescriptor createTableDescriptor(Table table, String[] columnNames, String[] columnTypes,
      String[] partitionColumns) {
    return new HiveTableDescriptor(HiveTableDescriptor.HIVE_DEFAULT_DATABASE_NAME, table.getTableName(), columnNames,
        columnTypes, partitionColumns, HiveTableDescriptor.HIVE_DEFAULT_DELIMITER,
        HiveTableDescriptor.HIVE_DEFAULT_SERIALIZATION_LIB_NAME, null, true, 1);
  }

  private List<TupleEntry> readTupleEntries(Tap tap) throws IOException {
    List<TupleEntry> result = new ArrayList<TupleEntry>();
    // FlowProcess<JobConf> flowProcess = createFlowProcess();
    JobConf conf = new JobConf();
    FlowProcess<JobConf> flowProcess = new HadoopFlowProcess(conf);
    tap.sourceConfInit(flowProcess, conf);
    TupleEntryIterator tupleEntryIterator = tap.openForRead(flowProcess);
    while (tupleEntryIterator.hasNext()) {
      TupleEntry tupleEntry = tupleEntryIterator.next();
      result.add(new TupleEntry(tupleEntry));
      System.out.println("Cascading>>> " + tupleEntry);
    }
    tupleEntryIterator.close();
    return result;
  }

}
