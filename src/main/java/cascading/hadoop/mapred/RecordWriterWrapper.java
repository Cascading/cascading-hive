/*
* This file has been created with ideas taken from Twitter's elephant-bird v4.13:
*
* https://github.com/twitter/elephant-bird/tree/elephant-bird-4.13
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

package cascading.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

class RecordWriterWrapper<K, V> implements org.apache.hadoop.mapred.RecordWriter<K, V>
  {

  private final RecordWriter<K, V> recordWriter;
  private final TaskAttemptContext taskAttemptContext;

  RecordWriterWrapper( RecordWriter<K, V> recordWriter, TaskAttemptContext taskAttemptContext )
    {
    this.recordWriter = recordWriter;
    this.taskAttemptContext = taskAttemptContext;
    }

  @Override
  public void close( Reporter reporter ) throws IOException
    {
    try
      {
      recordWriter.close( taskAttemptContext );
      }
    catch( InterruptedException e )
      {
      throw new IOException( e );
      }
    }

  @Override
  public void write( K key, V value ) throws IOException
    {
    try
      {
      recordWriter.write( key, value );
      }
    catch( InterruptedException e )
      {
      throw new IOException( e );
      }
    }

  }