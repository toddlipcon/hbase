/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.EnumMap;

public class FaultInjector {
  static EnumMap<Point, Fault> injected =
    new EnumMap<Point, Fault>(Point.class);

  public static Fault THROW_IOEXCEPTION = new Fault() {
    public void run(Object... args) throws IOException {
      throw new IOException("Injected fault");
    }
  };
  
  public enum Point {
    COMPACTION_BEFORE_RENAME, COMPACTION_DELETE_OLD
  }
  
  public static void runPoint(Point point, Object ...args)
    throws IOException {
    Fault f = injected.get(point);
    if (f != null) {
      f.run(args);
    }
  }
  
  public static void inject(Point p, Fault f) {
    injected.put(p, f);
  }
  
  public static void clear() {
    injected.clear();
  }
  
  public static Fault throwOnNthInvocation(final int n) {
    return new Fault() {
      private int invocation = 0;
      @Override
      public void run(Object... args) throws IOException {
        if (invocation++ == n) {
          throw new IOException("Injected fault");
        }
      }
    };
  }

  public interface Fault {
    public void run(Object... args) throws IOException;
  }
}
