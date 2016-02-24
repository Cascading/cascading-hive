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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

class InputSplitWrapper implements InputSplit, Configurable {

  org.apache.hadoop.mapreduce.InputSplit inputSplit;
  private Configuration conf;

  public InputSplitWrapper() {
  }

  public InputSplitWrapper(org.apache.hadoop.mapreduce.InputSplit inputSplit) {
    this.inputSplit = inputSplit;
  }

  @Override
  public long getLength() throws IOException {
    try {
      return inputSplit.getLength();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public String[] getLocations() throws IOException {
    try {
      return inputSplit.getLocations();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    inputSplit = deserializeInputSplit(conf, (DataInputStream) in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    serializeInputSplit(conf, (DataOutputStream) out, inputSplit);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public static void serializeInputSplit(Configuration conf, DataOutputStream out,
      org.apache.hadoop.mapreduce.InputSplit inputSplit2)
    throws IOException {
    Class<? extends InputSplit> clazz = inputSplit2.getClass().asSubclass(InputSplit.class);
    Text.writeString(out, clazz.getName());
    SerializationFactory factory = new SerializationFactory(conf);
    Serializer serializer = factory.getSerializer(clazz);
    serializer.open(out instanceof UncloseableDataOutputStream ? out : new UncloseableDataOutputStream(out));
    serializer.serialize(inputSplit2);
  }

  public static org.apache.hadoop.mapreduce.InputSplit deserializeInputSplit(Configuration conf, DataInputStream in)
    throws IOException {
    String name = Text.readString(in);
    Class<? extends org.apache.hadoop.mapreduce.InputSplit> clazz;
    try {
      clazz = conf.getClassByName(name).asSubclass(org.apache.hadoop.mapreduce.InputSplit.class);
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not find class for deserialized class name: " + name, e);
    }
    return deserializeInputSplitInternal(conf,
        in instanceof UncloseableDataInputStream ? in : new UncloseableDataInputStream(in), clazz);
  }

  private static <T extends org.apache.hadoop.mapreduce.InputSplit> T deserializeInputSplitInternal(Configuration conf,
      DataInputStream in, Class<T> clazz)
    throws IOException {
    T split = ReflectionUtils.newInstance(clazz, conf);
    SerializationFactory factory = new SerializationFactory(conf);
    Deserializer<T> deserializer = factory.getDeserializer(clazz);
    deserializer.open(in instanceof UncloseableDataInputStream ? in : new UncloseableDataInputStream(in));
    return deserializer.deserialize(split);
  }

  private static class UncloseableDataOutputStream extends DataOutputStream {
    public UncloseableDataOutputStream(DataOutputStream os) {
      super(os);
    }

    @Override
    public void close() {
      // We don't want classes given this stream to close it
    }
  }

  private static class UncloseableDataInputStream extends DataInputStream {
    public UncloseableDataInputStream(DataInputStream is) {
      super(is);
    }

    @Override
    public void close() {
      // We don't want classes given this stream to close it
    }
  }

}