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
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.OutputCommitter;

public class OutputCommitterWrapper extends org.apache.hadoop.mapred.OutputCommitter
  {

  private static final String MAPRED_OUTPUT_COMMITTER_CLASS = "mapred.output.committer.class";

  @SuppressWarnings("rawtypes")
  private final OutputFormatWrapper outputFormat = new OutputFormatWrapper();

  public static void setOutputCommitter( Configuration conf )
    {
    conf.setClass( MAPRED_OUTPUT_COMMITTER_CLASS, OutputCommitterWrapper.class, OutputCommitter.class );
    }

  public static void unsetOutputCommitter( Configuration conf )
    {
    conf.unset( MAPRED_OUTPUT_COMMITTER_CLASS );
    }

  private OutputCommitter getOutputCommitter( TaskAttemptContext taskAttemptContext ) throws IOException
    {
    try
      {
      Configuration conf = taskAttemptContext.getConfiguration();
      return outputFormat.getOutputFormat( conf ).getOutputCommitter( taskAttemptContext );
      }
    catch( InterruptedException e )
      {
      throw new IOException( e );
      }
    }

  private OutputCommitter getOutputCommitter( JobContext jobContext ) throws IOException
    {
    try
      {
      Configuration conf = jobContext.getConfiguration();
      return outputFormat.getOutputFormat( conf ).getOutputCommitter( WrapperUtils.getTaskAttemptContext( jobContext ) );
      }
    catch( InterruptedException e )
      {
      throw new IOException( e );
      }
    }

  @Override
  public void setupJob( JobContext jobContext ) throws IOException
    {
    getOutputCommitter( jobContext ).setupJob( jobContext );
    }

  @Override
  public void setupTask( TaskAttemptContext taskAttemptContext ) throws IOException
    {
    getOutputCommitter( taskAttemptContext ).setupTask( taskAttemptContext );
    }

  @Override
  public boolean needsTaskCommit( TaskAttemptContext taskAttemptContext ) throws IOException
    {
    return getOutputCommitter( taskAttemptContext ).needsTaskCommit( taskAttemptContext );
    }

  @Override
  public void commitTask( TaskAttemptContext taskAttemptContext ) throws IOException
    {
    getOutputCommitter( taskAttemptContext ).commitTask( taskAttemptContext );
    }

  @Override
  public void abortTask( TaskAttemptContext taskAttemptContext ) throws IOException
    {
    getOutputCommitter( taskAttemptContext ).abortTask( taskAttemptContext );
    }

  @Override
  public void abortJob( JobContext jobContext, int status ) throws IOException
    {
    getOutputCommitter( jobContext ).abortJob( jobContext, WrapperUtils.getState( status ) );
    }

  @Override
  public void commitJob( JobContext jobContext ) throws IOException
    {
    unsetOutputCommitter( jobContext.getConfiguration() );
    getOutputCommitter( jobContext ).commitJob( jobContext );
    }

  }
