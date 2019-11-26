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

package io.confluent.ksql.function.udtf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class CubeTest {

  private Cube cubeUdtf = new Cube();

  @Test
  public void shouldCubeSingleColumn() {

    Object[] args = {1};
    List<List<Object>> result = cubeUdtf.cube(Arrays.asList(args));
    assertThat(result.size(), is(2));
    assertThat(result.get(0), is(Collections.singletonList(null)));
    assertThat(result.get(1), is(Lists.newArrayList(1)));

    Object[] oneNull = {null};
    result = cubeUdtf.cube(Arrays.asList(oneNull));
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(Arrays.asList(new String[]{null})));
  }

  @Test
  public void shouldCubeColumnsWithDifferentTypes() {

    Object[] args = {1, "foo"};
    List<List<Object>> result = cubeUdtf.cube(Arrays.asList(args));

    assertThat(result.size(), is(4));
    assertThat(result.get(0), is(Arrays.asList(null, null)));
    assertThat(result.get(1), is(Arrays.asList(null, "foo")));
    assertThat(result.get(2), is(Arrays.asList(1, null)));
    assertThat(result.get(3), is(Arrays.asList(1, "foo")));
  }

  @Test
  public void shouldHandleNulls() {

    Object[] oneNull = {1, null};
    List<List<Object>> result = cubeUdtf.cube(Arrays.asList(oneNull));
    assertThat(result.size(), is(2));
    assertThat(result.get(0), is(Arrays.asList(null, null)));
    assertThat(result.get(1), is(Arrays.asList(1, null)));

    Object[] allNull = {null, null};
    result = cubeUdtf.cube(Arrays.asList(allNull));
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(Arrays.asList(null, null)));
  }
}
