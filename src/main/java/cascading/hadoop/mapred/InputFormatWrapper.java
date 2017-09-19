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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

public class InputFormatWrapper<K, V> implements org.apache.hadoop.mapred.InputFormat<K, V>
  {

  private static final String INPUT_FORMAT_KEY_COPIER_CLASS = "input.format.key.copier.class";
  private static final String INPUT_FORMAT_VALUE_COPIER_CLASS = "input.format.value.copier.class";
  private static final String WRAPPED_MAPREDUCE_INPUT_FORMAT_CLASS = "wrapped.mapreduce.input.format.class";
  private static final String MAPRED_INPUT_FORMAT_CLASS = "mapred.input.format.class";

  protected InputFormat<K, V> inputFormat;
  protected InputFormatValueCopier<K> keyCopier = null;
  protected InputFormatValueCopier<V> valueCopier = null;

  public static <K, V> void setInputFormat( Configuration conf, Class<? extends InputFormat<K, V>> inputFormatClass )
    {
    setInputFormat( conf, inputFormatClass, null, null );
    }

  public static <K, V> void setInputFormat( Configuration conf, Class<? extends InputFormat<K, V>> inputFormatClass,
                                            Class<? extends InputFormatValueCopier<K>> keyCopierClass,
                                            Class<? extends InputFormatValueCopier<V>> valueCopierClass )
    {
    conf.setClass( WRAPPED_MAPREDUCE_INPUT_FORMAT_CLASS, inputFormatClass, InputFormat.class );
    conf.setClass( MAPRED_INPUT_FORMAT_CLASS, InputFormatWrapper.class, org.apache.hadoop.mapred.InputFormat.class );
    if( keyCopierClass != null )
      {
      conf.setClass( INPUT_FORMAT_KEY_COPIER_CLASS, keyCopierClass, InputFormatValueCopier.class );
      }
    if( valueCopierClass != null )
      {
      conf.setClass( INPUT_FORMAT_VALUE_COPIER_CLASS, valueCopierClass, InputFormatValueCopier.class );
      }
    }

  private <T> InputFormatValueCopier<T> getValueCopier( Configuration conf, String confProperty )
    {
    if( conf.get( confProperty ) != null )
      {
      @SuppressWarnings( "rawtypes" )
      Class<? extends InputFormatValueCopier> copierClass = conf.getClass( confProperty, null,
        InputFormatValueCopier.class );
      if( null != copierClass )
        {
        @SuppressWarnings( "unchecked" )
        InputFormatValueCopier<T> copier = ReflectionUtils.newInstance( copierClass, conf );
        return copier;
        }
      }
    return null;
    }

  @SuppressWarnings("unchecked")
  InputFormat<K, V> getInputFormat( Configuration conf )
    {
    if( inputFormat == null )
      {
      @SuppressWarnings("rawtypes")
      Class<? extends InputFormat> inputFormatClass = conf.getClass( WRAPPED_MAPREDUCE_INPUT_FORMAT_CLASS, null,
        InputFormat.class );
      inputFormat = ReflectionUtils.newInstance( inputFormatClass, conf );
      keyCopier = getValueCopier( conf, INPUT_FORMAT_KEY_COPIER_CLASS );
      valueCopier = getValueCopier( conf, INPUT_FORMAT_VALUE_COPIER_CLASS );
      }
    return inputFormat;
    }

  @Override
  public RecordReader<K, V> getRecordReader( InputSplit split, JobConf job, Reporter reporter ) throws IOException
    {
    return new RecordReaderWrapper<K, V>( getInputFormat( job ), split, job, reporter, keyCopier, valueCopier );
    }

  @Override
  public InputSplit[] getSplits( JobConf job, int numSplits ) throws IOException
    {
    try
      {
      List<org.apache.hadoop.mapreduce.InputSplit> splits = getInputFormat( job )
        .getSplits( new JobContextImpl( job, null ) );

      if( splits == null )
        {
        return null;
        }

      InputSplit[] resultSplits = new InputSplit[ splits.size() ];
      int i = 0;
      for( org.apache.hadoop.mapreduce.InputSplit split : splits )
        {
        if( split.getClass() == org.apache.hadoop.mapreduce.lib.input.FileSplit.class )
          {
          org.apache.hadoop.mapreduce.lib.input.FileSplit mapreduceFileSplit = ( (org.apache.hadoop.mapreduce.lib.input.FileSplit) split );
          resultSplits[ i++ ] = new FileSplit( mapreduceFileSplit.getPath(), mapreduceFileSplit.getStart(),
            mapreduceFileSplit.getLength(), mapreduceFileSplit.getLocations() );
          }
        else
          {
          InputSplitWrapper wrapper = new InputSplitWrapper( split );
          wrapper.setConf( job );
          resultSplits[ i++ ] = wrapper;
          }
        }

      return resultSplits;

      }
    catch( InterruptedException e )
      {
      throw new IOException( e );
      }
    }
  }
