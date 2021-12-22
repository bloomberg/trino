/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.launcher.suite.suites;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeSnowflake;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

<<<<<<<< HEAD:testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/suite/suites/SuiteAllConnectorsSmoke.java
/**
 * Suite that verifies that the cluster starts with as many connectors
 * enabled as possible. The catalogs do not have to have valid configuration,
 * so it might not be possible to execute queries using them.
 */
public class SuiteAllConnectorsSmoke
========
public class SuiteSnowflake
>>>>>>>> da8fe892a7 (Add Snowflake JDBC Connector):testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/suite/suites/SuiteSnowflake.java
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                testOnEnvironment(EnvMultinodeSnowflake.class)
                        .withGroups("configured_features", "snowflake")
                        .build());
    }
}
