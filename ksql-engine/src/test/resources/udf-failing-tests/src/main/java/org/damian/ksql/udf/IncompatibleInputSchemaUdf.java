/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.damian.ksql.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;

/**
 * Class used to test the loading of UDFs. This is packaged in udf-failing-tests.jar
 * Attention: This test crashes the UdfLoader.
 */

@UdfDescription(
    name = "IncompatibleInputSchemaUdf",
    description = "A test-only UDF for testing validation")

public class IncompatibleInputSchemaUdf {

  @Udf
  public String foo(@UdfParameter(schema="ARRAY<VARCHAR>") final Map<String, String> p) {
    return "lala";
  }
}
