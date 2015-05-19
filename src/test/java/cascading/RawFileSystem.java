/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

public class RawFileSystem extends RawLocalFileSystem {
  private static final URI NAME;
  static {
    try {
      NAME = new URI("raw:///");
    } catch (URISyntaxException se) {
      throw new IllegalArgumentException("bad uri", se);
    }
  }

  @Override
  public URI getUri() {
    return NAME;
  }


  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    File file = pathToFile(path);
    if (!file.exists()) {
      throw new FileNotFoundException("Can't find " + path);
    }
    // get close enough
    short mod = 0;
    if (file.canRead()) {
      mod |= 0444;
    }
    if (file.canWrite()) {
      mod |= 0200;
    }
    if (file.canExecute()) {
      mod |= 0111;
    }
    return new FileStatus(file.length(), file.isDirectory(), 1, 1024,
        file.lastModified(), file.lastModified(),
        FsPermission.createImmutable(mod), "owen", "users", path);
  }
}