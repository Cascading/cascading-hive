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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

final class WrapperUtils
  {

  private WrapperUtils()
    {
    }

  static TaskAttemptContext getTaskAttemptContext( Configuration conf )
    {
    TaskAttemptID taskAttemptID = TaskAttemptID.forName( conf.get( "mapred.task.id" ) );
    return new TaskAttemptContextImpl( conf, taskAttemptID );
    }

  static TaskAttemptContext getTaskAttemptContext( JobContext jobContext )
    {
    Configuration conf = jobContext.getConfiguration();
    TaskID taskId = new org.apache.hadoop.mapreduce.TaskID( jobContext.getJobID(), TaskType.MAP, 0 );
    TaskAttemptID taskAttemptId = new org.apache.hadoop.mapreduce.TaskAttemptID( taskId, 0 );
    return new TaskAttemptContextImpl( conf, taskAttemptId );
    }

  static State getState( int status )
    {
    for( State value : State.values() )
      {
      if( value.getValue() == status )
        {
        return value;
        }
      }
    throw new IllegalStateException( "Unknown status: " + status );
    }

  }
