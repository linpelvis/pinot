/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;

public class RealtimeDictionaryProvider {

  public static MutableDictionaryReader getDictionaryFor(FieldSpec spec) {
    switch (spec.getDataType()) {
      case INT:
        return new IntMutableDictionary(spec);
      case LONG:
        return new LongMutableDictionary(spec);
      case FLOAT:
        return new FloatMutableDictionary(spec);
      case DOUBLE:
        return new DoubleMutableDictionary(spec);
      case BOOLEAN:
      case STRING:
        return new StringMutableDictionary(spec);
    }
    throw new UnsupportedOperationException();
  }
}
