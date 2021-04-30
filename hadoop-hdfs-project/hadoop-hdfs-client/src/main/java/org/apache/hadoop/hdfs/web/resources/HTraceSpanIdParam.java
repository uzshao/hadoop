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
package org.apache.hadoop.hdfs.web.resources;

import java.util.regex.Pattern;

/** policy parameter. */
public class HTraceSpanIdParam extends StringParam {
  /** Parameter name. */
  public static final String NAME = "htrace_spanid";
  /** Default parameter value. */
  public static final String DEFAULT = "";

  /**
   * Format of the param comes from {@link org.apache.htrace.core.SpanId#toString}
   */
  private static final Domain DOMAIN = new Domain(NAME, Pattern.compile("[0-9a-fA-F]{32}"));

  /**
   * Constructor.
   *
   * @param str
   *          a string representation of the parameter value.
   */
  public HTraceSpanIdParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT) ? null : str);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
