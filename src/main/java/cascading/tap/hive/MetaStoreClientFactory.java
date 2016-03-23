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

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

/** Utility class for creating {@link IMetaStoreClient} instances. Assists with mocking. */
class MetaStoreClientFactory implements Serializable
  {

  private static final long serialVersionUID = 1L;

  MetaStoreClientFactory()
    {
    }

  /**
   * Create an IMetaStore client.
   *
   * @return a new IMetaStoreClient
   * @throws MetaException in case the creation fails.
   */
  IMetaStoreClient newInstance( Configuration conf ) throws MetaException
    {
    // it is a bit unclear if it is safe to re-use these instances, so we create a
    // new one every time, to be sure
    HiveConf hiveConf = HiveConfFactory.getHiveConf( conf );

    return RetryingMetaStoreClient.getProxy( hiveConf, new HiveMetaHookLoader()
      {
      @Override
      public HiveMetaHook getHook( Table tbl ) throws MetaException
        {
        return null;
        }
      }, HiveMetaStoreClient.class.getName() );
    }

  }
