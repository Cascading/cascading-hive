package cascading.tap.hive;

import static org.apache.hadoop.hive.metastore.api.LockState.ABORT;
import static org.apache.hadoop.hive.metastore.api.LockState.ACQUIRED;
import static org.apache.hadoop.hive.metastore.api.LockState.NOT_ACQUIRED;
import static org.apache.hadoop.hive.metastore.api.LockState.WAITING;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cascading.CascadingException;
import cascading.flow.Flow;
import cascading.tap.hive.LockManager.HeartbeatFactory;
import cascading.tap.hive.LockManager.HeartbeatTimerTask;
import cascading.tap.hive.LockManager.LockFailureListener;

import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.class)
public class LockManagerTest {

  private static final HiveTableDescriptor TABLE_1 = new HiveTableDescriptor("DB", "ONE", new String[] { "A" },
      new String[] { "STRING" });
  private static final HiveTableDescriptor TABLE_2 = new HiveTableDescriptor("DB", "TWO", new String[] { "B" },
      new String[] { "STRING" });
  private static final List<HiveTableDescriptor> TABLES = ImmutableList.of(TABLE_1, TABLE_2);
  private static final long LOCK_ID = 42;

  @Mock
  private MetaStoreClientFactory mockMetaStoreClientFactory;
  @Mock
  private IMetaStoreClient mockMetaStoreClient;
  @Mock
  private LockFailureListener mockListener;
  @Mock
  private Flow mockFlow;
  @Mock
  private LockResponse mockLockResponse;
  @Mock
  private HeartbeatFactory mockHeartbeatFactory;
  @Mock
  private Timer mockHeartbeat;
  @Captor
  private ArgumentCaptor<LockRequest> requestCaptor;

  private LockManager lockManager;
  private Configuration configuration = new Configuration();

  @Before
  public void injectMocks() throws Exception {
    when(mockFlow.getConfig()).thenReturn(configuration);
    when(mockMetaStoreClientFactory.newInstance(any(Configuration.class))).thenReturn(mockMetaStoreClient);
    when(mockMetaStoreClient.lock(any(LockRequest.class))).thenReturn(mockLockResponse);
    when(mockLockResponse.getLockid()).thenReturn(LOCK_ID);
    when(mockLockResponse.getState()).thenReturn(ACQUIRED);
    when(
        mockHeartbeatFactory.newInstance(any(MetaStoreClientFactory.class), any(LockFailureListener.class),
            any(Configuration.class), any(Iterable.class), anyLong(), anyInt())).thenReturn(mockHeartbeat);

    lockManager = new LockManager(mockMetaStoreClientFactory, mockHeartbeatFactory, mockListener, TABLES, 3, 0);
  }

  @Test
  public void testAcquireLockWithNoIssues() {
    lockManager.acquireLock(configuration);
    assertEquals(Long.valueOf(LOCK_ID), lockManager.getLockId());
    verify(mockMetaStoreClient).close();
  }

  @Test
  public void testAcquireLockCheckHeartbeatCreated() {
    configuration.set("hive.txn.timeout", "100s");
    lockManager.acquireLock(configuration);

    verify(mockHeartbeatFactory).newInstance(eq(mockMetaStoreClientFactory), eq(mockListener),
        any(Configuration.class), eq(TABLES), eq(LOCK_ID), eq(75));
  }

  @Test
  public void testAcquireLockCheckUser() throws Exception {
    lockManager.acquireLock(configuration);
    verify(mockMetaStoreClient).lock(requestCaptor.capture());
    LockRequest actualRequest = requestCaptor.getValue();
    assertEquals(UserGroupInformation.getCurrentUser().getUserName(), actualRequest.getUser());
  }

  @Test
  public void testAcquireLockCheckLocks() throws Exception {
    lockManager.acquireLock(configuration);
    verify(mockMetaStoreClient).lock(requestCaptor.capture());
    List<LockComponent> components = requestCaptor.getValue().getComponent();
    assertEquals(2, components.size());
    assertEquals("db", components.get(0).getDbname());
    assertEquals("one", components.get(0).getTablename());
    assertEquals(LockType.SHARED_READ, components.get(0).getType());
    assertEquals(LockLevel.TABLE, components.get(0).getLevel());
    assertEquals("db", components.get(1).getDbname());
    assertEquals("two", components.get(1).getTablename());
    assertEquals(LockType.SHARED_READ, components.get(1).getType());
    assertEquals(LockLevel.TABLE, components.get(1).getLevel());
  }

  @Test(expected = CascadingException.class)
  public void testAcquireLockNotAcquired() {
    when(mockLockResponse.getState()).thenReturn(NOT_ACQUIRED);
    lockManager.acquireLock(configuration);
  }

  @Test
  public void testAcquireLockNotAcquiredCheckClose() {
    when(mockLockResponse.getState()).thenReturn(NOT_ACQUIRED);
    try {
      lockManager.acquireLock(configuration);
    } catch (CascadingException e) {
      verify(mockMetaStoreClient).close();
    }
  }

  @Test(expected = CascadingException.class)
  public void testAcquireLockAborted() {
    when(mockLockResponse.getState()).thenReturn(ABORT);
    lockManager.acquireLock(configuration);
  }

  @Test
  public void testAcquireLockAbortedCheckClose() {
    when(mockLockResponse.getState()).thenReturn(ABORT);
    try {
      lockManager.acquireLock(configuration);
    } catch (CascadingException e) {
      verify(mockMetaStoreClient).close();
    }
  }

  @Test(expected = CascadingException.class)
  public void testAcquireLockWithWaitRetriesExceeded() {
    when(mockLockResponse.getState()).thenReturn(WAITING, WAITING, WAITING);
    lockManager.acquireLock(configuration);
  }

  @Test
  public void testAcquireLockWithWaitRetriesExceededCheckClose() {
    when(mockLockResponse.getState()).thenReturn(WAITING, WAITING, WAITING);
    try {
      lockManager.acquireLock(configuration);
    } catch (CascadingException e) {
      verify(mockMetaStoreClient, times(3)).close();
    }
  }

  @Test
  public void testAcquireLockWithWaitRetries() {
    when(mockLockResponse.getState()).thenReturn(WAITING, WAITING, ACQUIRED);
    lockManager.acquireLock(configuration);
    assertEquals(Long.valueOf(LOCK_ID), lockManager.getLockId());
  }

  @Test
  public void testReleaseLock() throws Exception {
    lockManager.acquireLock(configuration);
    lockManager.releaseLock(configuration);
    verify(mockMetaStoreClient).unlock(LOCK_ID);
    verify(mockMetaStoreClient, times(2)).close();
  }

  @Test
  public void testReleaseLockNoLock() throws Exception {
    lockManager.releaseLock(configuration);
    verify(mockMetaStoreClient).close();
    verifyNoMoreInteractions(mockMetaStoreClient);
  }

  @Test
  public void testReleaseLockCancelsHeartbeat() {
    lockManager.acquireLock(configuration);
    lockManager.releaseLock(configuration);
    verify(mockHeartbeat).cancel();
  }

  @Test
  public void testHeartbeat() throws Exception {
    HeartbeatTimerTask task = new LockManager.HeartbeatTimerTask(mockMetaStoreClientFactory, mockListener,
        configuration, TABLES, LOCK_ID);
    task.run();
    verify(mockMetaStoreClient).heartbeat(0, LOCK_ID);
    verify(mockMetaStoreClient).close();
  }

  @Test
  public void testHeartbeatFailsNoSuchLockException() throws Exception {
    Throwable t = new NoSuchLockException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(0, LOCK_ID);
    HeartbeatTimerTask task = new LockManager.HeartbeatTimerTask(mockMetaStoreClientFactory, mockListener,
        configuration, TABLES, LOCK_ID);
    task.run();
    verify(mockListener).lockFailed(LOCK_ID, TABLES, t);
    verify(mockMetaStoreClient).close();
  }

  @Test
  public void testHeartbeatFailsNoSuchTxnException() throws Exception {
    Throwable t = new NoSuchTxnException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(0, LOCK_ID);
    HeartbeatTimerTask task = new LockManager.HeartbeatTimerTask(mockMetaStoreClientFactory, mockListener,
        configuration, TABLES, LOCK_ID);
    task.run();
    verify(mockListener).lockFailed(LOCK_ID, TABLES, t);
    verify(mockMetaStoreClient).close();
  }

  @Test
  public void testHeartbeatFailsTxnAbortedException() throws Exception {
    Throwable t = new TxnAbortedException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(0, LOCK_ID);
    HeartbeatTimerTask task = new LockManager.HeartbeatTimerTask(mockMetaStoreClientFactory, mockListener,
        configuration, TABLES, LOCK_ID);
    task.run();
    verify(mockListener).lockFailed(LOCK_ID, TABLES, t);
    verify(mockMetaStoreClient).close();
  }

  @Test
  public void testHeartbeatContinuesTException() throws Exception {
    Throwable t = new TException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(0, LOCK_ID);
    HeartbeatTimerTask task = new LockManager.HeartbeatTimerTask(mockMetaStoreClientFactory, mockListener,
        configuration, TABLES, LOCK_ID);
    task.run();
    verifyZeroInteractions(mockListener);
    verify(mockMetaStoreClient).close();
  }

  @Test
  public void testOnStarting() {
    lockManager.onStarting(mockFlow);
    assertEquals(Long.valueOf(LOCK_ID), lockManager.getLockId());
  }

  @Test
  public void testOnCompleted() throws Exception {
    lockManager.onStarting(mockFlow);
    lockManager.onCompleted(mockFlow);
    verify(mockMetaStoreClient).unlock(LOCK_ID);
  }
  
  @Test
  public void testOnStopping() throws Exception {
    lockManager.onStarting(mockFlow);
    lockManager.onStopping(mockFlow);
    verify(mockMetaStoreClient, atLeastOnce()).unlock(LOCK_ID);
  }
  
  @Test
  public void testFullLifecycle() throws Exception {
    lockManager.onStarting(mockFlow);
    lockManager.onStopping(mockFlow);
    lockManager.onCompleted(mockFlow);
    verify(mockMetaStoreClient, atLeastOnce()).unlock(LOCK_ID);
  }

  @Test
  public void testOnThrowable() {
    assertEquals(false, lockManager.onThrowable(mockFlow, new Throwable()));
  }

  @Test
  public void testGetTableNames() {
    String tableNames = LockManager.getTableNames(TABLES);
    assertEquals("db.one, db.two", tableNames);
  }

  @Test
  public void testManagedFlowListenerStopsFlow() {
    LockFailureListener listener = new LockManager.ManagedFlowListener(mockFlow);
    listener.lockFailed(LOCK_ID, TABLES, new Throwable());
    verify(mockFlow).stop();
  }

}
