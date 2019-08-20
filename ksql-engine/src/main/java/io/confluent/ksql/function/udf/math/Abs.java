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

package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

@UdfDescription(name = "Abs", description = Abs.DESCRIPTION)
public class Abs {

  static final String DESCRIPTION = "Returns the absolute value of its argument. If the argument "
      + "is not negative, the argument is returned. If the argument is negative, the negation of "
      + "the argument is returned.";


  @Udf
  public Integer abs(@UdfParameter final Integer val) {
    return (val == null) ? null : Math.abs(val);
  }

  @Udf
  public Long abs(@UdfParameter final Long val) {
    return (val == null) ? null : Math.abs(val);
  }

  @Udf
  public Double abs(@UdfParameter final Double val) {
    return (val == null) ? null : Math.abs(val);
  }

  @Udf(schemaProvider = "provideSchema")
  public BigDecimal abs(@UdfParameter final BigDecimal val) {
    return (val == null) ? null : val.abs();
  }

  @UdfSchemaProvider
  public Schema provideSchema(final List<Schema> params) {
    final int scale = Integer.parseInt(params.get(0).parameters().get("scale"));
    final int precision = Integer.parseInt(params.get(0).parameters()
        .get("connect.decimal.precision"));
    return DecimalUtil.builder(precision, scale).build();
  }
}