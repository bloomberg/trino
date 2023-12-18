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
package io.trino.plugin.opa;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.blackhole.BlackHolePlugin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.plugin.opa.FunctionalHelpers.Pair;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Testcontainers
@TestInstance(PER_CLASS)
public class TestOpaAccessControlSystem
{
    private DistributedQueryRunnerHelper runner;

    private static final String OPA_ALLOW_POLICY_NAME = "allow";
    private static final String OPA_BATCH_ALLOW_POLICY_NAME = "batchAllow";
    @Container
    private static final OpaContainer OPA_CONTAINER = new OpaContainer();

    @Nested
    @TestInstance(PER_CLASS)
    @DisplayName("Unbatched Authorizer Tests")
    class UnbatchedAuthorizerTests
    {
        @BeforeAll
        public void setupTrino()
                throws Exception
        {
            setupTrinoWithOpa(new TestHelpers.OpaConfigBuilder()
                    .withBasePolicy(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ALLOW_POLICY_NAME))
                    .buildConfig());
        }

        @AfterAll
        public void teardown()
        {
            runner.teardown();
        }

        @ParameterizedTest(name = "{index}: {0}")
        @MethodSource("io.trino.plugin.opa.TestOpaAccessControlSystem#filterSchemaTests")
        public void testAllowsQueryAndFilters(String userName, Set<String> expectedCatalogs)
                throws IOException, InterruptedException
        {
            OPA_CONTAINER.submitPolicy("""
                    package trino
                    import future.keywords.in
                    import future.keywords.if

                    default allow = false
                    allow {
                      is_bob
                      can_be_accessed_by_bob
                    }
                    allow if is_admin

                    is_admin {
                      input.context.identity.user == "admin"
                    }
                    is_bob {
                      input.context.identity.user == "bob"
                    }
                    can_be_accessed_by_bob {
                      input.action.operation in ["ImpersonateUser", "ExecuteQuery"]
                    }
                    can_be_accessed_by_bob {
                      input.action.operation in ["FilterCatalogs", "AccessCatalog"]
                      input.action.resource.catalog.name == "catalog_one"
                    }
                    """);
            Set<String> catalogs = runner.querySetOfStrings(userName, "SHOW CATALOGS");
            assertThat(catalogs).containsExactlyInAnyOrderElementsOf(expectedCatalogs);
        }

        @Test
        public void testShouldDenyQueryIfDirected()
                throws IOException, InterruptedException
        {
            OPA_CONTAINER.submitPolicy("""
                    package trino
                    import future.keywords.in
                    default allow = false

                    allow {
                        input.context.identity.user in ["someone", "admin"]
                    }
                    """);
            assertThatThrownBy(() -> runner.querySetOfStrings("bob", "SHOW CATALOGS"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Access Denied");
            // smoke test: we can still query if we are the right user
            runner.querySetOfStrings("admin", "SHOW CATALOGS");
        }
    }

    @Nested
    @TestInstance(PER_CLASS)
    @DisplayName("Batched Authorizer Tests")
    class BatchedAuthorizerTests
    {
        @BeforeAll
        public void setupTrino()
                throws Exception
        {
            setupTrinoWithOpa(new TestHelpers.OpaConfigBuilder()
                    .withBasePolicy(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_ALLOW_POLICY_NAME))
                    .withBatchPolicy(OPA_CONTAINER.getOpaUriForPolicyPath(OPA_BATCH_ALLOW_POLICY_NAME))
                    .buildConfig());
        }

        @AfterAll
        public void teardown()
        {
            runner.teardown();
        }

        @ParameterizedTest(name = "{index}: {0}")
        @MethodSource("io.trino.plugin.opa.TestOpaAccessControlSystem#filterSchemaTests")
        public void testFilterOutItemsBatch(String userName, Set<String> expectedCatalogs)
                throws IOException, InterruptedException
        {
            OPA_CONTAINER.submitPolicy("""
                    package trino
                    import future.keywords.in
                    import future.keywords.if
                    default allow = false

                    allow if is_admin

                    allow {
                        is_bob
                        input.action.operation in ["AccessCatalog", "ExecuteQuery", "ImpersonateUser", "ShowSchemas", "SelectFromColumns"]
                    }

                    is_bob {
                        input.context.identity.user == "bob"
                    }

                    is_admin {
                        input.context.identity.user == "admin"
                    }

                    batchAllow[i] {
                        some i
                        is_bob
                        input.action.operation == "FilterCatalogs"
                        input.action.filterResources[i].catalog.name == "catalog_one"
                    }

                    batchAllow[i] {
                        some i
                        input.action.filterResources[i]
                        is_admin
                    }
                    """);
            Set<String> catalogs = runner.querySetOfStrings(userName, "SHOW CATALOGS");
            assertThat(catalogs).containsExactlyInAnyOrderElementsOf(expectedCatalogs);
        }

        @Test
        public void testDenyUnbatchedQuery()
                throws IOException, InterruptedException
        {
            OPA_CONTAINER.submitPolicy("""
                    package trino
                    import future.keywords.in
                    default allow = false
                    """);
            assertThatThrownBy(() -> runner.querySetOfStrings("bob", "SELECT version()"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Access Denied");
        }

        @Test
        public void testAllowUnbatchedQuery()
                throws IOException, InterruptedException
        {
            OPA_CONTAINER.submitPolicy("""
                    package trino
                    import future.keywords.in
                    default allow = false
                    allow {
                        input.context.identity.user == "bob"
                        input.action.operation in ["ImpersonateUser", "ExecuteFunction", "AccessCatalog", "ExecuteQuery"]
                    }
                    """);
            Set<String> version = runner.querySetOfStrings("bob", "SELECT version()");
            assertThat(version).isNotEmpty();
        }
    }

    private void setupTrinoWithOpa(Map<String, String> opaConfig)
            throws Exception
    {
        this.runner = DistributedQueryRunnerHelper.withOpaConfig(opaConfig);
        runner.getBaseQueryRunner().installPlugin(new BlackHolePlugin());
        runner.getBaseQueryRunner().createCatalog("catalog_one", "blackhole");
        runner.getBaseQueryRunner().createCatalog("catalog_two", "blackhole");
    }

    private static Stream<Arguments> filterSchemaTests()
    {
        Stream<Pair<String, Set<String>>> userAndExpectedCatalogs = Stream.of(
                Pair.of("bob", ImmutableSet.of("catalog_one")),
                Pair.of("admin", ImmutableSet.of("catalog_one", "catalog_two", "system")));
        return userAndExpectedCatalogs.map(testCase -> Arguments.of(Named.of(testCase.first(), testCase.first()), testCase.second()));
    }
}
