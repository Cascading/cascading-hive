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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

public class OutputFormatWrapper<K, V> implements org.apache.hadoop.mapred.OutputFormat<K, V>
  {

  private static final String WRAPPED_MAPREDUCE_OUTPUT_FORMAT_CLASS = "wrapped.mapreduce.output.format.class";
  private static final String MAPRED_OUTPUT_FORMAT_CLASS = "mapred.output.format.class";

  protected OutputFormat<K, V> outputFormat;

  public static <K, V> void setOutputFormat( Configuration conf, Class<? extends OutputFormat<K, V>> ouputFormatClass )
    {
    conf.setClass( WRAPPED_MAPREDUCE_OUTPUT_FORMAT_CLASS, ouputFormatClass, OutputFormat.class );
    conf.setClass( MAPRED_OUTPUT_FORMAT_CLASS, OutputFormatWrapper.class, org.apache.hadoop.mapred.OutputFormat.class );
    }

  @SuppressWarnings("unchecked")
  OutputFormat<K, V> getOutputFormat( Configuration conf )
    {
    if( outputFormat == null )
      {
      @SuppressWarnings("rawtypes")
      Class<? extends OutputFormat> outputFormatClass = conf.getClass( WRAPPED_MAPREDUCE_OUTPUT_FORMAT_CLASS, null,
        OutputFormat.class );
      outputFormat = ReflectionUtils.newInstance( outputFormatClass, conf );
      }
    return outputFormat;
    }

  @Override
  public void checkOutputSpecs( FileSystem ignored, JobConf conf ) throws IOException
    {
    try
      {
      JobContext jobContext = new JobContextImpl( conf, null );
      getOutputFormat( conf ).checkOutputSpecs( jobContext );
      }
    catch( InterruptedException e )
      {
      throw new IOException( e );
      }
    }

  @Override
  public RecordWriter<K, V> getRecordWriter( FileSystem ignored, JobConf conf, String name, Progressable progress )
    throws IOException
    {
    try
      {
      TaskAttemptContext taskAttemptContext = WrapperUtils.getTaskAttemptContext( conf );
      org.apache.hadoop.mapreduce.RecordWriter<K, V> recordWriter = getOutputFormat( conf )
        .getRecordWriter( taskAttemptContext );
      return new RecordWriterWrapper<K, V>( recordWriter, taskAttemptContext );
      }
    catch( InterruptedException e )
      {
      throw new IOException( e );
      }
    }

  }