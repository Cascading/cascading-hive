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

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import cascading.CascadingException;
import cascading.flow.Flow;
import cascading.flow.FlowListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the state required to safely read from an ACID table.
 */
public class LockManager implements FlowListener
  {

  private static final Logger LOG = LoggerFactory.getLogger( LockManager.class );
  private final MetaStoreClientFactory metaStoreClientFactory;
  private final HeartbeatFactory heartbeatFactory;
  private final LockFailureListener listener;
  private final Iterable<HiveTableDescriptor> tableDescriptors;
  private final int lockRetries;
  private final int retryWaitSeconds;
  private Timer heartbeat;
  private Long lockId;

  LockManager( MetaStoreClientFactory metaStoreClientFactory, HeartbeatFactory heartbeatFactory,
               LockFailureListener listener, Iterable<HiveTableDescriptor> tableDescriptors, int lockRetries,
               int retryWaitSeconds )
    {
    this.metaStoreClientFactory = metaStoreClientFactory;
    this.heartbeatFactory = heartbeatFactory;
    this.tableDescriptors = tableDescriptors;
    this.listener = listener;
    this.lockRetries = lockRetries;
    this.retryWaitSeconds = retryWaitSeconds;
    }

  static String getTableNames( Iterable<HiveTableDescriptor> descriptors )
    {
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for( HiveTableDescriptor descriptor : descriptors )
      {
      if( first )
        first = false;
      else
        builder.append( ", " );

      builder.append( descriptor.getDatabaseName() );
      builder.append( '.' );
      builder.append( descriptor.getTableName() );
      }
    return builder.toString();
    }

  /** Attempts to acquire a read lock on the table, returns if successful, throws exception otherwise. */
  void acquireLock( Configuration conf ) throws CascadingException
    {
    lockId = internalAcquireLock( conf );
    initiateHeartbeat( conf );
    }

  /** Attempts to release the read lock on the table. Throws an exception if the lock failed at any point. */
  void releaseLock( Configuration conf ) throws CascadingException
    {
    IMetaStoreClient metaStoreClient = null;
    try
      {
      metaStoreClient = metaStoreClientFactory.newInstance( conf );
      if( heartbeat != null )
        heartbeat.cancel();

      if( lockId != null )
        {
        metaStoreClient.unlock( lockId );
        LOG.debug( "Released lock " + lockId );
        lockId = null;
        }
      }
    catch( TException e )
      {
      LOG.error( "Lock " + lockId + " failed.", e );
      listener.lockFailed( lockId, tableDescriptors, e );
      }
    finally
      {
      if( metaStoreClient != null )
        metaStoreClient.close();
      }
    }

  private long internalAcquireLock( Configuration conf ) throws CascadingException
    {
    int attempts = 0;
    do
      {
      IMetaStoreClient metaStoreClient = null;
      LockRequest request = buildSharedReadLockRequest( conf );
      LockResponse response = null;
      try
        {
        metaStoreClient = metaStoreClientFactory.newInstance( conf );
        response = metaStoreClient.lock( request );
        }
      catch( TException e )
        {
        throw new CascadingException( "Unable to acquire lock for tables: [" + getTableNames( tableDescriptors ) + "]", e );
        }
      finally
        {
        if( metaStoreClient != null )
          metaStoreClient.close();
        }
      if( response != null )
        {
        LockState state = response.getState();
        if( state == LockState.NOT_ACQUIRED || state == LockState.ABORT )
        // I expect we'll only see NOT_ACQUIRED here?
          break;
        if( state == LockState.ACQUIRED )
          {
          LOG.debug( "Acquired lock " + response.getLockid() );
          return response.getLockid();
          }
        if( state == LockState.WAITING )
          {
          try
            {
            Thread.sleep( TimeUnit.SECONDS.toMillis( retryWaitSeconds ) );
            }
          catch( InterruptedException e )
            {
            // ignore
            }
          }
        }
      attempts++;
      }
    while( attempts < lockRetries );

    throw new CascadingException( "Could not acquire lock on tables: [" + getTableNames( tableDescriptors ) + "]" );
    }

  private LockRequest buildSharedReadLockRequest( Configuration conf )
    {
    String user = getUser( conf );
    LockRequestBuilder requestBuilder = new LockRequestBuilder();
    for( HiveTableDescriptor descriptor : tableDescriptors )
      {
      LockComponent component = new LockComponentBuilder()
        .setDbName( descriptor.getDatabaseName() )
        .setTableName( descriptor.getTableName() )
        .setShared()
        .build();
      requestBuilder.addLockComponent( component );
      }
    LockRequest request = requestBuilder.setUser( user ).build();
    return request;
    }

  private String getUser( Configuration conf ) throws CascadingException
    {
    HiveConf hiveConf = HiveConfFactory.getHiveConf( conf );
    try
      {
      String user = hiveConf.getUser();
      LOG.debug( "Resolved Hive user: {}", user );
      return user;
      }
    catch( IOException e )
      {
      throw new CascadingException( "Unable to determine user.", e );
      }
    }

  private void initiateHeartbeat( Configuration conf )
    {
    HiveConf hiveConf = HiveConfFactory.getHiveConf( conf );
    String txTimeoutSeconds = hiveConf.getVar( HiveConf.ConfVars.HIVE_TXN_TIMEOUT );
    int heartbeatPeriod;
    if( txTimeoutSeconds != null )
      {
      // We want to send the heartbeat at an interval that is less than the timeout
      heartbeatPeriod = Math.max( 1,
        (int) ( Integer.parseInt( txTimeoutSeconds.substring( 0, txTimeoutSeconds.length() - 1 ) ) * 0.75 ) );
      }
    else
      heartbeatPeriod = 275;

    LOG.debug( "Heartbeat period {}s", heartbeatPeriod );
    heartbeat = heartbeatFactory.newInstance( metaStoreClientFactory, listener, hiveConf, tableDescriptors, lockId,
      heartbeatPeriod );
    }

  /** Acquires shared read locks on Hive ACID tables. */
  @Override
  public void onStarting( Flow flow )
    {
    LOG.debug( "onStarting called on flow listener." );
    acquireLock( (Configuration) flow.getConfig() );
    }

  /** Releases locks on Hive ACID tables. */
  @Override
  public void onStopping( Flow flow )
    {
    LOG.debug( "onStopping called on flow listener." );
    releaseLock( (Configuration) flow.getConfig() );
    }

  /** Releases locks on Hive ACID tables. */
  @Override
  public void onCompleted( Flow flow )
    {
    LOG.debug( "onCompleted called on flow listener." );
    releaseLock( (Configuration) flow.getConfig() );
    }

  @Override
  public boolean onThrowable( Flow flow, Throwable throwable )
    {
    return false;
    }

  Long getLockId()
    {
    return lockId;
    }

  /** Provides a means to handle the situation when a held lock fails. */
  public interface LockFailureListener
    {
    static final LockFailureListener NULL_LISTENER = new LockFailureListener()
      {
      @Override
      public void lockFailed( long lockId, Iterable<HiveTableDescriptor> tableDescriptors, Throwable t )
        {
        LOG.warn( "Ignored lock failure: lockId=" + lockId + ", tables=" + getTableNames( tableDescriptors ), t );
        }
      };

    /** Called when the specified lock has failed. You should probably abort your job in this case. */
    void lockFailed( long lockId, Iterable<HiveTableDescriptor> tableDescriptors, Throwable t );
    }

  static final class ManagedFlowListener implements LockFailureListener
    {
    private Flow managedFlow;

    ManagedFlowListener( Flow managedFlow )
      {
      this.managedFlow = managedFlow;
      }

    @Override
    public void lockFailed( long lockId, Iterable<HiveTableDescriptor> tableDescriptors, Throwable t )
      {
      LOG.error( "Stopping flow '" + managedFlow.getName() + "', lock " + lockId + " failed on tables: ["
        + getTableNames( tableDescriptors ) + "]", t );
      managedFlow.stop();
      }
    }

  /** Constructs a lock manager for a set of Hive ACID tables from which we wish to read. */
  public static class Builder
    {
    private Set<HiveTableDescriptor> descriptors = new LinkedHashSet<>();
    private LockFailureListener listener = LockFailureListener.NULL_LISTENER;
    private int lockRetries = 5;
    private int retryWaitSeconds = 30;

    /** Adds a table for which a shared read lock will be requested. */
    public Builder addTable( HiveTableDescriptor descriptor )
      {
      if( descriptor == null )
        throw new NullPointerException( "desriptor" );

      if( descriptor.isTransactional() )
        descriptors.add( descriptor );
      else
        LOG.warn( "Table '{}.{}' is not transactional - will not be included in lock.", descriptor.getDatabaseName(),
          descriptor.getTableName() );

      return this;
      }

    /** Sets a listener to handle failures of locks that were previously acquired. */
    public Builder lockFailureListener( LockFailureListener listener )
      {
      if( listener == null )
        throw new NullPointerException( "listener" );

      this.listener = listener;
      return this;
      }

    /** Creates a handler that stops the provided flow should a previously acquired lock fail. */
    public Builder managedFlow( Flow flow )
      {
      if( flow == null )
        throw new NullPointerException( "flow" );

      this.listener = new ManagedFlowListener( flow );
      return this;
      }

    public Builder lockRetries( int lockRetries )
      {
      if( lockRetries < 1 )
        throw new IllegalArgumentException( "lockRetries must be larger than 0, got " + lockRetries );
      this.lockRetries = lockRetries;
      return this;
      }

    public Builder retryWaitSeconds( int retryWaitSeconds )
      {
      if( retryWaitSeconds < 1 )
        throw new IllegalArgumentException( "retryWaitSeconds must be larger than 0, got " + retryWaitSeconds );
      this.retryWaitSeconds = retryWaitSeconds;
      return this;
      }

    public LockManager build()
      {
      if( LockFailureListener.NULL_LISTENER.equals( listener ) )
        LOG.warn( "No {} supplied. Data quality and availability cannot be assured.",
          LockFailureListener.class.getSimpleName() );

      return new LockManager( new MetaStoreClientFactory(), new HeartbeatFactory(), listener, descriptors, lockRetries,
        retryWaitSeconds );
      }
    }

  static class HeartbeatFactory
    {
    public Timer newInstance( MetaStoreClientFactory metaStoreClientFactory, LockFailureListener listener,
                              Configuration conf, Iterable<HiveTableDescriptor> tableDescriptors, long lockId, int heartbeatPeriod )
      {
      Timer heartbeatTimer = new Timer( "hive-lock-heartbeat[lockId=" + lockId + "]", true );
      HeartbeatTimerTask task = new HeartbeatTimerTask( metaStoreClientFactory, listener, conf, tableDescriptors, lockId );
      heartbeatTimer.schedule( task, TimeUnit.SECONDS.toMillis( heartbeatPeriod ),
        TimeUnit.SECONDS.toMillis( heartbeatPeriod ) );
      return heartbeatTimer;
      }
    }

  static final class HeartbeatTimerTask extends TimerTask
    {
    private final MetaStoreClientFactory metaStoreClientFactory;
    private final Configuration conf;
    private final long lockId;
    private final LockFailureListener listener;
    private final Iterable<HiveTableDescriptor> tableDescriptors;

    HeartbeatTimerTask( MetaStoreClientFactory metaStoreClientFactory, LockFailureListener listener, Configuration conf,
                        Iterable<HiveTableDescriptor> tableDescriptors, long lockId )
      {
      this.metaStoreClientFactory = metaStoreClientFactory;
      this.listener = listener;
      this.conf = conf;
      this.tableDescriptors = tableDescriptors;
      this.lockId = lockId;
      }

    @Override
    public void run()
      {
      IMetaStoreClient metaStoreClient = null;
      try
        {
        metaStoreClient = metaStoreClientFactory.newInstance( conf );
        // I'm assuming that there is no transaction ID for a read lock.
        metaStoreClient.heartbeat( 0L, lockId );
        LOG.debug( "Sent heartbeat for lock " + lockId + "." );
        }
      catch( NoSuchLockException e )
        {
        failLock( e );
        }
      catch( NoSuchTxnException e )
        {
        failLock( e );
        }
      catch( TxnAbortedException e )
        {
        failLock( e );
        }
      catch( TException e )
        {
        LOG.warn( "Failed to send heartbeat to meta store.", e );
        }
      finally
        {
        if( metaStoreClient != null )
          metaStoreClient.close();
        }
      }

    private void failLock( Exception e )
      {
      LOG.debug( "Lock " + lockId + " failed.", e );
      // Cancel the heartbeat
      cancel();
      listener.lockFailed( lockId, tableDescriptors, e );
      }
    }

  }
