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

package cascading.tap.hive;

import java.util.Arrays;
import java.util.List;
import java.util.Timer;

import cascading.CascadingException;
import cascading.flow.Flow;
import cascading.tap.hive.LockManager.HeartbeatFactory;
import cascading.tap.hive.LockManager.HeartbeatTimerTask;
import cascading.tap.hive.LockManager.LockFailureListener;
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

import static org.apache.hadoop.hive.metastore.api.LockState.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class LockManagerTest
  {

  private static final HiveTableDescriptor TABLE_1 = new HiveTableDescriptor( "DB", "ONE", new String[]{"A"},
    new String[]{"STRING"} );
  private static final HiveTableDescriptor TABLE_2 = new HiveTableDescriptor( "DB", "TWO", new String[]{"B"},
    new String[]{"STRING"} );
  private static final List<HiveTableDescriptor> TABLES = Arrays.asList( TABLE_1, TABLE_2 );
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
  public void injectMocks() throws Exception
    {
    when( mockFlow.getConfig() ).thenReturn( configuration );
    when( mockMetaStoreClientFactory.newInstance( any( Configuration.class ) ) ).thenReturn( mockMetaStoreClient );
    when( mockMetaStoreClient.lock( any( LockRequest.class ) ) ).thenReturn( mockLockResponse );
    when( mockLockResponse.getLockid() ).thenReturn( LOCK_ID );
    when( mockLockResponse.getState() ).thenReturn( ACQUIRED );
    when(
      mockHeartbeatFactory.newInstance( any( MetaStoreClientFactory.class ), any( LockFailureListener.class ),
        any( Configuration.class ), any( Iterable.class ), anyLong(), anyInt() ) ).thenReturn( mockHeartbeat );

    lockManager = new LockManager( mockMetaStoreClientFactory, mockHeartbeatFactory, mockListener, TABLES, 3, 0 );
    }

  @Test
  public void testAcquireLockWithNoIssues()
    {
    lockManager.acquireLock( configuration );
    assertEquals( Long.valueOf( LOCK_ID ), lockManager.getLockId() );
    verify( mockMetaStoreClient ).close();
    }

  @Test
  public void testAcquireLockCheckHeartbeatCreated()
    {
    configuration.set( "hive.txn.timeout", "100s" );
    lockManager.acquireLock( configuration );

    verify( mockHeartbeatFactory ).newInstance( eq( mockMetaStoreClientFactory ), eq( mockListener ),
      any( Configuration.class ), eq( TABLES ), eq( LOCK_ID ), eq( 75 ) );
    }

  @Test
  public void testAcquireLockCheckUser() throws Exception
    {
    lockManager.acquireLock( configuration );
    verify( mockMetaStoreClient ).lock( requestCaptor.capture() );
    LockRequest actualRequest = requestCaptor.getValue();
    assertEquals( UserGroupInformation.getCurrentUser().getUserName(), actualRequest.getUser() );
    }

  @Test
  public void testAcquireLockCheckLocks() throws Exception
    {
    lockManager.acquireLock( configuration );
    verify( mockMetaStoreClient ).lock( requestCaptor.capture() );
    List<LockComponent> components = requestCaptor.getValue().getComponent();

    assertEquals( 2, components.size() );

    LockComponent expected1 = new LockComponent( LockType.SHARED_READ, LockLevel.TABLE, "db" );
    expected1.setTablename( "one" );
    assertTrue( components.contains( expected1 ) );

    LockComponent expected2 = new LockComponent( LockType.SHARED_READ, LockLevel.TABLE, "db" );
    expected2.setTablename( "two" );
    assertTrue( components.contains( expected2 ) );
    }

  @Test(expected = CascadingException.class)
  public void testAcquireLockNotAcquired()
    {
    when( mockLockResponse.getState() ).thenReturn( NOT_ACQUIRED );
    lockManager.acquireLock( configuration );
    }

  @Test
  public void testAcquireLockNotAcquiredCheckClose()
    {
    when( mockLockResponse.getState() ).thenReturn( NOT_ACQUIRED );
    try
      {
      lockManager.acquireLock( configuration );
      }
    catch( CascadingException e )
      {
      verify( mockMetaStoreClient ).close();
      }
    }

  @Test(expected = CascadingException.class)
  public void testAcquireLockAborted()
    {
    when( mockLockResponse.getState() ).thenReturn( ABORT );
    lockManager.acquireLock( configuration );
    }

  @Test
  public void testAcquireLockAbortedCheckClose()
    {
    when( mockLockResponse.getState() ).thenReturn( ABORT );
    try
      {
      lockManager.acquireLock( configuration );
      }
    catch( CascadingException e )
      {
      verify( mockMetaStoreClient ).close();
      }
    }

  @Test(expected = CascadingException.class)
  public void testAcquireLockWithWaitRetriesExceeded()
    {
    when( mockLockResponse.getState() ).thenReturn( WAITING, WAITING, WAITING );
    lockManager.acquireLock( configuration );
    }

  @Test
  public void testAcquireLockWithWaitRetriesExceededCheckClose()
    {
    when( mockLockResponse.getState() ).thenReturn( WAITING, WAITING, WAITING );
    try
      {
      lockManager.acquireLock( configuration );
      }
    catch( CascadingException e )
      {
      verify( mockMetaStoreClient, times( 3 ) ).close();
      }
    }

  @Test
  public void testAcquireLockWithWaitRetries()
    {
    when( mockLockResponse.getState() ).thenReturn( WAITING, WAITING, ACQUIRED );
    lockManager.acquireLock( configuration );
    assertEquals( Long.valueOf( LOCK_ID ), lockManager.getLockId() );
    }

  @Test
  public void testReleaseLock() throws Exception
    {
    lockManager.acquireLock( configuration );
    lockManager.releaseLock( configuration );
    verify( mockMetaStoreClient ).unlock( LOCK_ID );
    verify( mockMetaStoreClient, times( 2 ) ).close();
    }

  @Test
  public void testReleaseLockNoLock() throws Exception
    {
    lockManager.releaseLock( configuration );
    verify( mockMetaStoreClient ).close();
    verifyNoMoreInteractions( mockMetaStoreClient );
    }

  @Test
  public void testReleaseLockCancelsHeartbeat()
    {
    lockManager.acquireLock( configuration );
    lockManager.releaseLock( configuration );
    verify( mockHeartbeat ).cancel();
    }

  @Test
  public void testHeartbeat() throws Exception
    {
    HeartbeatTimerTask task = new LockManager.HeartbeatTimerTask( mockMetaStoreClientFactory, mockListener,
      configuration, TABLES, LOCK_ID );
    task.run();
    verify( mockMetaStoreClient ).heartbeat( 0, LOCK_ID );
    verify( mockMetaStoreClient ).close();
    }

  @Test
  public void testHeartbeatFailsNoSuchLockException() throws Exception
    {
    Throwable t = new NoSuchLockException();
    doThrow( t ).when( mockMetaStoreClient ).heartbeat( 0, LOCK_ID );
    HeartbeatTimerTask task = new LockManager.HeartbeatTimerTask( mockMetaStoreClientFactory, mockListener,
      configuration, TABLES, LOCK_ID );
    task.run();
    verify( mockListener ).lockFailed( LOCK_ID, TABLES, t );
    verify( mockMetaStoreClient ).close();
    }

  @Test
  public void testHeartbeatFailsNoSuchTxnException() throws Exception
    {
    Throwable t = new NoSuchTxnException();
    doThrow( t ).when( mockMetaStoreClient ).heartbeat( 0, LOCK_ID );
    HeartbeatTimerTask task = new LockManager.HeartbeatTimerTask( mockMetaStoreClientFactory, mockListener,
      configuration, TABLES, LOCK_ID );
    task.run();
    verify( mockListener ).lockFailed( LOCK_ID, TABLES, t );
    verify( mockMetaStoreClient ).close();
    }

  @Test
  public void testHeartbeatFailsTxnAbortedException() throws Exception
    {
    Throwable t = new TxnAbortedException();
    doThrow( t ).when( mockMetaStoreClient ).heartbeat( 0, LOCK_ID );
    HeartbeatTimerTask task = new LockManager.HeartbeatTimerTask( mockMetaStoreClientFactory, mockListener,
      configuration, TABLES, LOCK_ID );
    task.run();
    verify( mockListener ).lockFailed( LOCK_ID, TABLES, t );
    verify( mockMetaStoreClient ).close();
    }

  @Test
  public void testHeartbeatContinuesTException() throws Exception
    {
    Throwable t = new TException();
    doThrow( t ).when( mockMetaStoreClient ).heartbeat( 0, LOCK_ID );
    HeartbeatTimerTask task = new LockManager.HeartbeatTimerTask( mockMetaStoreClientFactory, mockListener,
      configuration, TABLES, LOCK_ID );
    task.run();
    verifyZeroInteractions( mockListener );
    verify( mockMetaStoreClient ).close();
    }

  @Test
  public void testOnStarting()
    {
    lockManager.onStarting( mockFlow );
    assertEquals( Long.valueOf( LOCK_ID ), lockManager.getLockId() );
    }

  @Test
  public void testOnCompleted() throws Exception
    {
    lockManager.onStarting( mockFlow );
    lockManager.onCompleted( mockFlow );
    verify( mockMetaStoreClient ).unlock( LOCK_ID );
    }

  @Test
  public void testOnStopping() throws Exception
    {
    lockManager.onStarting( mockFlow );
    lockManager.onStopping( mockFlow );
    verify( mockMetaStoreClient, atLeastOnce() ).unlock( LOCK_ID );
    }

  @Test
  public void testFullLifecycle() throws Exception
    {
    lockManager.onStarting( mockFlow );
    lockManager.onStopping( mockFlow );
    lockManager.onCompleted( mockFlow );
    verify( mockMetaStoreClient, atLeastOnce() ).unlock( LOCK_ID );
    }

  @Test
  public void testOnThrowable()
    {
    assertEquals( false, lockManager.onThrowable( mockFlow, new Throwable() ) );
    }

  @Test
  public void testGetTableNames()
    {
    String tableNames = LockManager.getTableNames( TABLES );
    assertEquals( "db.one, db.two", tableNames );
    }

  @Test
  public void testManagedFlowListenerStopsFlow()
    {
    LockFailureListener listener = new LockManager.ManagedFlowListener( mockFlow );
    listener.lockFailed( LOCK_ID, TABLES, new Throwable() );
    verify( mockFlow ).stop();
    }

  }
