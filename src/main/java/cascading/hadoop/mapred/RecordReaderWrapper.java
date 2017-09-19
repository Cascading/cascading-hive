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

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.MapContextImpl;

class RecordReaderWrapper<K, V> implements RecordReader<K, V>
  {

  private org.apache.hadoop.mapreduce.RecordReader<K, V> recordReader;

  private final long splitLen; // for getPos()

  private K key = null;
  private V value = null;
  private final InputFormatValueCopier<K> keyCopier;
  private final InputFormatValueCopier<V> valueCopier;

  private boolean firstRecord = false;
  private boolean eof = false;

  public RecordReaderWrapper( InputFormat<K, V> newInputFormat, InputSplit oldSplit, JobConf oldJobConf,
                              Reporter reporter, InputFormatValueCopier<K> keyCopier,
                              InputFormatValueCopier<V> valueCopier ) throws IOException
    {
    this.keyCopier = keyCopier;
    this.valueCopier = valueCopier;
    splitLen = oldSplit.getLength();

    org.apache.hadoop.mapreduce.InputSplit split;
    if( oldSplit.getClass() == FileSplit.class )
      {
      split = new org.apache.hadoop.mapreduce.lib.input.FileSplit( ( (FileSplit) oldSplit ).getPath(),
        ( (FileSplit) oldSplit ).getStart(), ( (FileSplit) oldSplit ).getLength(), oldSplit.getLocations() );
      }
    else
      {
      split = ( (InputSplitWrapper) oldSplit ).inputSplit;
      }

    TaskAttemptID taskAttemptID = TaskAttemptID.forName( oldJobConf.get( "mapred.task.id" ) );
    if( taskAttemptID == null )
      {
      taskAttemptID = new TaskAttemptID();
      }

    TaskAttemptContext taskContext = new MapContextImpl<>( oldJobConf, taskAttemptID, null, null, null,
      new ReporterWrapper( reporter ), null );
    try
      {
      recordReader = newInputFormat.createRecordReader( split, taskContext );
      recordReader.initialize( split, taskContext );
      }
    catch( InterruptedException e )
      {
      throw new IOException( e );
      }
    }

  private void initKeyValueObjects()
    {
    // read once to gain access to key and value objects
    try
      {
      if( !firstRecord & !eof )
        {
        if( recordReader.nextKeyValue() )
          {
          firstRecord = true;
          key = recordReader.getCurrentKey();
          value = recordReader.getCurrentValue();
          }
        else
          {
          eof = true;
          }
        }
      }
    catch( Exception e )
      {
      throw new RuntimeException( "Could not read first record (and it was not an EOF)", e );
      }
    }

  @Override
  public void close() throws IOException
    {
    recordReader.close();
    }

  @Override
  public K createKey()
    {
    initKeyValueObjects();
    return key;
    }

  @Override
  public V createValue()
    {
    initKeyValueObjects();
    return value;
    }

  @Override
  public long getPos() throws IOException
    {
    return (long) ( splitLen * getProgress() );
    }

  @Override
  public float getProgress() throws IOException
    {
    try
      {
      return recordReader.getProgress();
      }
    catch( InterruptedException e )
      {
      throw new IOException( e );
      }
    }

  @Override
  public boolean next( K key, V value ) throws IOException
    {
    if( eof )
      {
      return false;
      }

    if( firstRecord )
      { // key & value are already read.
      firstRecord = false;
      return true;
      }

    try
      {
      if( recordReader.nextKeyValue() )
        {

        if( key != recordReader.getCurrentKey() )
          {
          if( keyCopier == null )
            {
            throw new IOException( "InputFormatWrapper only supports RecordReaders that return the same key objects "
              + "unless a InputFormatValueCopier is provided. Current reader class : " + recordReader.getClass() );
            }
          keyCopier.copyValue( key, recordReader.getCurrentKey() );
          }

        if( value != recordReader.getCurrentValue() )
          {
          if( valueCopier == null )
            {
            throw new IOException( "InputFormatWrapper only supports RecordReaders that return the same value objects "
              + "unless a InputFormatValueCopier is provided. Current reader class : " + recordReader.getClass() );
            }
          valueCopier.copyValue( value, recordReader.getCurrentValue() );
          }

        return true;
        }
      }
    catch( InterruptedException e )
      {
      throw new IOException( e );
      }

    eof = true; // strictly not required, just for consistency
    return false;
    }
  }