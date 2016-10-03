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
package com.linkedin.pinot.common.request.transform;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class for transformation functions.
 */
public class TransformFunctionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformFunctionFactory.class);

  private static final Map<String, TransformFunction> TRANSFORM_FUNCTION_MAP = new HashMap<>();
  private static boolean _inited = false;

  /**
   * Private constructor, to prevent instantiation.
   */
  private TransformFunctionFactory() {

  }

  /**
   * This method builds the transform factory containing functions
   * specified in the server configuration. Throws {@link RuntimeException} if it has
   * already been initialized. Method is synchronized (as opposed to concurrent), as it is
   * expected to be called only once, during start-up.
   *
   * @param transformFunctions Array of transform function names
   */
  @SuppressWarnings("unchecked")
  public static synchronized void init(@Nonnull String[] transformFunctions) {
    // Already initialized, nothing to be done.
    if (_inited) {
      return;
    }

    _inited = true;
    for (String functionName : transformFunctions) {
      try {

        Class<TransformFunction> functionClass = (Class<TransformFunction>) Class.forName(functionName);
        TransformFunction transformFunction = functionClass.newInstance();
        TRANSFORM_FUNCTION_MAP.put(transformFunction.getName().toLowerCase(), transformFunction);
      } catch (ClassNotFoundException e) {
        LOGGER.error("Could not find class for transform function '{}'", functionName, e);
      } catch (InstantiationException e) {
        LOGGER.error("Could not instantiate class for transform function '{}", functionName, e);
      } catch (IllegalAccessException e) {
        LOGGER.error("Could not access members of class for transform function '{}", functionName, e);
      }
    }
  }

  /**
   * Returns an instance of TransformFunction for the given name. Returns null if
   * function name was not found, is null, or could not instantiate the class.
   *
   * @param functionName Transform function name
   * @return TransformFunction for the given functionName
   */
  public static TransformFunction get(String functionName) {
    if (functionName == null) {
      return null;
    }

    Preconditions.checkState(_inited, "TransformFunctionFactory not initialized.");
    String key = functionName.toLowerCase();

    if (!TRANSFORM_FUNCTION_MAP.containsKey(key)) {
      return null;
    } else {
      return TRANSFORM_FUNCTION_MAP.get(key);
    }
  }
}
