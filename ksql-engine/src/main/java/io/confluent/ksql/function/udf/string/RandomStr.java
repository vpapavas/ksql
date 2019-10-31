/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.Random;

@UdfDescription(name = "randomstr", description = RandomStr.DESCRIPTION)
public class RandomStr {

  static final String DESCRIPTION = "RANDOMSTR(min, max) returns a random string with atleast `min`"
      + " characters and atmost `max` characters.";

  private static final long SEED = 183839573947549L;
  private static final Random random = new Random(SEED);

  private static String byteArrayToHex(final byte[] a) {
    final StringBuilder sb = new StringBuilder(a.length * 2);
    for (byte b: a) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  @Udf
  public String randomstr(
      @UdfParameter(description = "minimum number of characters in string") final int min,
      @UdfParameter(description = "maximum number of characters in string") final int max
  ) {
    if (min < 1 || max < min) {
      return null;
    }

    final int len = min + random.nextInt(max - min + 1);
    final byte[] bytes = new byte[len / 2];
    random.nextBytes(bytes);
    return byteArrayToHex(bytes);
  }
}
