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

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.StatusReporter;

public class ReporterWrapper extends StatusReporter implements Reporter {
  private final Reporter reporter;

  public ReporterWrapper(Reporter reporter) {
    this.reporter = reporter;
  }

  @Override
  public Counters.Counter getCounter(Enum<?> name) {
    return reporter.getCounter(name);
  }

  @Override
  public Counters.Counter getCounter(String group, String name) {
    return reporter.getCounter(group, name);
  }

  @Override
  public void incrCounter(Enum<?> key, long amount) {
    reporter.incrCounter(key, amount);
  }

  @Override
  public void incrCounter(String group, String counter, long amount) {
    reporter.incrCounter(group, counter, amount);
  }

  @Override
  public InputSplit getInputSplit() throws UnsupportedOperationException {
    return reporter.getInputSplit();
  }

  @Override
  public void progress() {
    reporter.progress();
  }

  @Override
  public float getProgress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStatus(String status) {
    reporter.setStatus(status);
  }
}