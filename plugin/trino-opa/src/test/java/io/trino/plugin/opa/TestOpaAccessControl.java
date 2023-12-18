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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.trino.plugin.opa.FunctionalHelpers.Pair;
import io.trino.plugin.opa.HttpClientUtils.InstrumentedHttpClient;
import io.trino.plugin.opa.HttpClientUtils.MockResponse;
import io.trino.plugin.opa.TestHelpers.MethodWrapper;
import io.trino.plugin.opa.TestHelpers.TestingSystemAccessControlContext;
import io.trino.plugin.opa.schema.OpaViewExpression;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.trino.plugin.opa.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.opa.RequestTestUtilities.buildValidatingRequestHandler;
import static io.trino.plugin.opa.TestHelpers.BAD_REQUEST_RESPONSE;
import static io.trino.plugin.opa.TestHelpers.MALFORMED_RESPONSE;
import static io.trino.plugin.opa.TestHelpers.NO_ACCESS_RESPONSE;
import static io.trino.plugin.opa.TestHelpers.OK_RESPONSE;
import static io.trino.plugin.opa.TestHelpers.SERVER_ERROR_RESPONSE;
import static io.trino.plugin.opa.TestHelpers.UNDEFINED_RESPONSE;
import static io.trino.plugin.opa.TestHelpers.createFailingTestCases;
import static io.trino.plugin.opa.TestHelpers.createMockHttpClient;
import static io.trino.plugin.opa.TestHelpers.createOpaAuthorizer;
import static io.trino.plugin.opa.TestHelpers.systemSecurityContextFromIdentity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpaAccessControl
{
    private static final URI OPA_SERVER_URI = URI.create("http://my-uri/");
    private static final URI OPA_SERVER_ROW_FILTERING_URI = URI.create("http://my-row-filtering-uri");
    private static final URI OPA_SERVER_COLUMN_MASK_URI = URI.create("http://my-column-masking-uri");
    private static final Identity TEST_IDENTITY = Identity.forUser("source-user").withGroups(ImmutableSet.of("some-group")).build();
    private static final SystemSecurityContext TEST_SECURITY_CONTEXT = systemSecurityContextFromIdentity(TEST_IDENTITY);
    private static final Map<String, String> OPA_CONFIG_WITH_ONLY_ALLOW = new TestHelpers.OpaConfigBuilder().withBasePolicy(OPA_SERVER_URI).buildConfig();
    // The below identity and security ctx would go away if we move all the tests to use their static constant counterparts above
    private final Identity requestingIdentity = Identity.ofUser("source-user");
    private final SystemSecurityContext requestingSecurityContext = systemSecurityContextFromIdentity(requestingIdentity);

    @Test
    public void testResponseHasExtraFields()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, 200,"""
                {
                    "result": true,
                    "decision_id": "foo",
                    "some_debug_info": {"test": ""}
                }"""));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);
        authorizer.checkCanExecuteQuery(requestingIdentity);
    }

    @Test
    public void testNoResourceAction()
    {
        testNoResourceAction("ExecuteQuery", OpaAccessControl::checkCanExecuteQuery);
        testNoResourceAction("ReadSystemInformation", OpaAccessControl::checkCanReadSystemInformation);
        testNoResourceAction("WriteSystemInformation", OpaAccessControl::checkCanWriteSystemInformation);
    }

    private void testNoResourceAction(String actionName, BiConsumer<OpaAccessControl, Identity> method)
    {
        Set<String> expectedRequests = ImmutableSet.of("""
                {
                    "operation": "%s"
                }""".formatted(actionName));
        TestHelpers.ThrowingMethodWrapper wrappedMethod = new TestHelpers.ThrowingMethodWrapper((accessControl) -> method.accept(accessControl, TEST_IDENTITY));
        assertAccessControlMethodBehaviour(wrappedMethod, expectedRequests);
    }

    private static Stream<Arguments> tableResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName>> methods = Stream.of(
                OpaAccessControl::checkCanShowCreateTable,
                OpaAccessControl::checkCanDropTable,
                OpaAccessControl::checkCanSetTableComment,
                OpaAccessControl::checkCanSetViewComment,
                OpaAccessControl::checkCanSetColumnComment,
                OpaAccessControl::checkCanShowColumns,
                OpaAccessControl::checkCanAddColumn,
                OpaAccessControl::checkCanDropColumn,
                OpaAccessControl::checkCanAlterColumn,
                OpaAccessControl::checkCanRenameColumn,
                OpaAccessControl::checkCanInsertIntoTable,
                OpaAccessControl::checkCanDeleteFromTable,
                OpaAccessControl::checkCanTruncateTable,
                OpaAccessControl::checkCanCreateView,
                OpaAccessControl::checkCanDropView,
                OpaAccessControl::checkCanRefreshMaterializedView,
                OpaAccessControl::checkCanDropMaterializedView);
        Stream<String> actions = Stream.of(
                "ShowCreateTable",
                "DropTable",
                "SetTableComment",
                "SetViewComment",
                "SetColumnComment",
                "ShowColumns",
                "AddColumn",
                "DropColumn",
                "AlterColumn",
                "RenameColumn",
                "InsertIntoTable",
                "DeleteFromTable",
                "TruncateTable",
                "CreateView",
                "DropView",
                "RefreshMaterializedView",
                "DropMaterializedView");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#tableResourceTestCases")
    public void testTableResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName> callable)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        callable.accept(
                authorizer,
                requestingSecurityContext,
                new CatalogSchemaTableName("my_catalog", "my_schema", "my_table"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "tableName": "my_table"
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableResourceFailureTestCases()
    {
        return createFailingTestCases(tableResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#tableResourceFailureTestCases")
    public void testTableResourceFailure(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName> method,
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        assertThatThrownBy(
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        new CatalogSchemaTableName("my_catalog", "my_schema", "my_table")))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    private static Stream<Arguments> tableWithPropertiesTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Map>> methods = Stream.of(
                OpaAccessControl::checkCanSetTableProperties,
                OpaAccessControl::checkCanSetMaterializedViewProperties,
                OpaAccessControl::checkCanCreateTable,
                OpaAccessControl::checkCanCreateMaterializedView);
        Stream<String> actions = Stream.of(
                "SetTableProperties",
                "SetMaterializedViewProperties",
                "CreateTable",
                "CreateMaterializedView");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#tableWithPropertiesTestCases")
    public void testTableWithPropertiesActions(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Map> callable)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        Map<String, Optional<Object>> properties = ImmutableMap.<String, Optional<Object>>builder()
                .put("string_item", Optional.of("string_value"))
                .put("empty_item", Optional.empty())
                .put("boxed_number_item", Optional.of(Integer.valueOf(32)))
                .buildOrThrow();

        callable.accept(authorizer, requestingSecurityContext, table, properties);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "tableName": "my_table",
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "properties": {
                                "string_item": "string_value",
                                "empty_item": null,
                                "boxed_number_item": 32
                            }
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableWithPropertiesFailureTestCases()
    {
        return createFailingTestCases(tableWithPropertiesTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#tableWithPropertiesFailureTestCases")
    public void testTableWithPropertiesActionFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Map> method,
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        assertThatThrownBy(
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        new CatalogSchemaTableName("my_catalog", "my_schema", "my_table"),
                        ImmutableMap.of()))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    private static Stream<Arguments> identityResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAccessControl, Identity, Identity>> methods = Stream.of(
                OpaAccessControl::checkCanViewQueryOwnedBy,
                OpaAccessControl::checkCanKillQueryOwnedBy);
        Stream<String> actions = Stream.of(
                "ViewQueryOwnedBy",
                "KillQueryOwnedBy");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#identityResourceTestCases")
    public void testIdentityResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, Identity, Identity> callable)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        Identity dummyIdentity = Identity.forUser("dummy-user")
                .withGroups(ImmutableSet.of("some-group"))
                .build();
        callable.accept(authorizer, requestingIdentity, dummyIdentity);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "user": {
                            "user": "dummy-user",
                            "groups": ["some-group"]
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> identityResourceFailureTestCases()
    {
        return createFailingTestCases(identityResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#identityResourceFailureTestCases")
    public void testIdentityResourceActionsFailure(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, Identity, Identity> method,
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        assertThatThrownBy(
                () -> method.accept(
                        authorizer,
                        requestingIdentity,
                        Identity.ofUser("dummy-user")))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    private static Stream<Arguments> stringResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, String>> methods = Stream.of(
                (accessControl, systemSecurityContext, argument) -> accessControl.checkCanSetSystemSessionProperty(systemSecurityContext.getIdentity(), argument),
                OpaAccessControl::checkCanCreateCatalog,
                OpaAccessControl::checkCanDropCatalog,
                OpaAccessControl::checkCanShowSchemas);
        Stream<Pair<String, String>> actionAndResource = Stream.of(
                Pair.of("SetSystemSessionProperty", "systemSessionProperty"),
                Pair.of("CreateCatalog", "catalog"),
                Pair.of("DropCatalog", "catalog"),
                Pair.of("ShowSchemas", "catalog"));
        return Streams.zip(
                actionAndResource,
                methods,
                (action, method) -> Arguments.of(Named.of(action.first(), action.first()), action.second(), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#stringResourceTestCases")
    public void testStringResourceAction(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, String> callable)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        callable.accept(authorizer, requestingSecurityContext, "resource_name");

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "%s": {
                            "name": "resource_name"
                        }
                    }
                }
                """.formatted(actionName, resourceName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> stringResourceFailureTestCases()
    {
        return createFailingTestCases(stringResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#stringResourceFailureTestCases")
    public void testStringResourceActionsFailure(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, String> method,
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        assertThatThrownBy(
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        "dummy_value"))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    public void testCanImpersonateUser()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        authorizer.checkCanImpersonateUser(requestingIdentity, "some_other_user");

        String expectedRequest = """
                {
                    "operation": "ImpersonateUser",
                    "resource": {
                        "user": {
                            "user": "some_other_user"
                        }
                    }
                }
                """;
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanImpersonateUserFailure(
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        assertThatThrownBy(
                () -> authorizer.checkCanImpersonateUser(requestingIdentity, "some_other_user"))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    public void testCanAccessCatalog()
    {
        InstrumentedHttpClient permissiveClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl permissiveAuthorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, permissiveClient);
        assertThat(permissiveAuthorizer.canAccessCatalog(requestingSecurityContext, "test_catalog")).isTrue();

        InstrumentedHttpClient restrictiveClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, NO_ACCESS_RESPONSE));
        OpaAccessControl restrictiveAuthorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, restrictiveClient);
        assertThat(restrictiveAuthorizer.canAccessCatalog(requestingSecurityContext, "test_catalog")).isFalse();

        String expectedRequest = """
                {
                    "operation": "AccessCatalog",
                    "resource": {
                        "catalog": {
                            "name": "test_catalog"
                        }
                    }
                }""";
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), permissiveClient.getRequests(), "/input/action");
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), restrictiveClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#illegalResponseArgumentProvider")
    public void testCanAccessCatalogIllegalResponses(
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        assertThatThrownBy(
                () -> authorizer.canAccessCatalog(requestingSecurityContext, "my_catalog"))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    private static Stream<Arguments> schemaResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaName>> methods = Stream.of(
                OpaAccessControl::checkCanDropSchema,
                OpaAccessControl::checkCanShowCreateSchema,
                OpaAccessControl::checkCanShowTables,
                OpaAccessControl::checkCanShowFunctions);
        Stream<String> actions = Stream.of(
                "DropSchema",
                "ShowCreateSchema",
                "ShowTables",
                "ShowFunctions");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#schemaResourceTestCases")
    public void testSchemaResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaName> callable)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        callable.accept(authorizer, requestingSecurityContext, new CatalogSchemaName("my_catalog", "my_schema"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "schema": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema"
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> schemaResourceFailureTestCases()
    {
        return createFailingTestCases(schemaResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#schemaResourceFailureTestCases")
    public void testSchemaResourceActionsFailure(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaName> method,
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        assertThatThrownBy(
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        new CatalogSchemaName("dummy_catalog", "dummy_schema")))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    public void testCreateSchema()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");
        authorizer.checkCanCreateSchema(requestingSecurityContext, schema, ImmutableMap.of("some_key", "some_value"));
        authorizer.checkCanCreateSchema(requestingSecurityContext, schema, ImmutableMap.of());

        Set<String> expectedRequests = ImmutableSet.<String>builder()
                .add("""
                    {
                        "operation": "CreateSchema",
                        "resource": {
                            "schema": {
                                "catalogName": "my_catalog",
                                "schemaName": "my_schema",
                                "properties": {
                                    "some_key": "some_value"
                                }
                            }
                        }
                    }
                    """)
                .add("""
                    {
                        "operation": "CreateSchema",
                        "resource": {
                            "schema": {
                                "catalogName": "my_catalog",
                                "schemaName": "my_schema",
                                "properties": {}
                            }
                        }
                    }
                    """)
                .build();
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#allErrorCasesArgumentProvider")
    public void testCreateSchemaFailure(
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        assertThatThrownBy(
                () -> authorizer.checkCanCreateSchema(
                        requestingSecurityContext,
                        new CatalogSchemaName("my_catalog", "my_schema"),
                        ImmutableMap.of()))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    public void testCanRenameSchema()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        CatalogSchemaName sourceSchema = new CatalogSchemaName("my_catalog", "my_schema");
        authorizer.checkCanRenameSchema(requestingSecurityContext, sourceSchema, "new_schema_name");

        String expectedRequest = """
                {
                    "operation": "RenameSchema",
                    "resource": {
                        "schema": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema"
                        }
                    },
                    "targetResource": {
                        "schema": {
                            "catalogName": "my_catalog",
                            "schemaName": "new_schema_name"
                        }
                    }
                }
                """;
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanRenameSchemaFailure(
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        assertThatThrownBy(
                () -> authorizer.checkCanRenameSchema(
                        requestingSecurityContext,
                        new CatalogSchemaName("my_catalog", "my_schema"),
                        "new_schema_name"))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    private static Stream<Arguments> renameTableTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName>> methods = Stream.of(
                OpaAccessControl::checkCanRenameTable,
                OpaAccessControl::checkCanRenameView,
                OpaAccessControl::checkCanRenameMaterializedView);
        Stream<String> actions = Stream.of(
                "RenameTable",
                "RenameView",
                "RenameMaterializedView");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#renameTableTestCases")
    public void testRenameTableActions(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName> method)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        CatalogSchemaTableName sourceTable = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        CatalogSchemaTableName targetTable = new CatalogSchemaTableName("my_catalog", "new_schema_name", "new_table_name");

        method.accept(authorizer, requestingSecurityContext, sourceTable, targetTable);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "tableName": "my_table"
                        }
                    },
                    "targetResource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "new_schema_name",
                            "tableName": "new_table_name"
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> renameTableFailureTestCases()
    {
        return createFailingTestCases(renameTableTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#renameTableFailureTestCases")
    public void testRenameTableFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName> method,
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        CatalogSchemaTableName sourceTable = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        CatalogSchemaTableName targetTable = new CatalogSchemaTableName("my_catalog", "new_schema_name", "new_table_name");
        assertThatThrownBy(
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        sourceTable,
                        targetTable))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    public void testCanSetSchemaAuthorization()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");

        authorizer.checkCanSetSchemaAuthorization(requestingSecurityContext, schema, new TrinoPrincipal(PrincipalType.USER, "my_user"));

        String expectedRequest = """
                {
                    "operation": "SetSchemaAuthorization",
                    "resource": {
                        "schema": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema"
                        }
                    },
                    "grantee": {
                        "name": "my_user",
                        "type": "USER"
                    }
                }
                """;
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanSetSchemaAuthorizationFailure(
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");
        assertThatThrownBy(
                () -> authorizer.checkCanSetSchemaAuthorization(
                        requestingSecurityContext,
                        schema,
                        new TrinoPrincipal(PrincipalType.USER, "my_user")))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    private static Stream<Arguments> setTableAuthorizationTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal>> methods = Stream.of(
                OpaAccessControl::checkCanSetTableAuthorization,
                OpaAccessControl::checkCanSetViewAuthorization);
        Stream<String> actions = Stream.of(
                "SetTableAuthorization",
                "SetViewAuthorization");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#setTableAuthorizationTestCases")
    public void testCanSetTableAuthorization(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal> method)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");

        method.accept(authorizer, requestingSecurityContext, table, new TrinoPrincipal(PrincipalType.USER, "my_user"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "tableName": "my_table"
                        }
                    },
                    "grantee": {
                        "name": "my_user",
                        "type": "USER"
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> setTableAuthorizationFailureTestCases()
    {
        return createFailingTestCases(setTableAuthorizationTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#setTableAuthorizationFailureTestCases")
    public void testCanSetTableAuthorizationFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal> method,
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");

        assertThatThrownBy(
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        table,
                        new TrinoPrincipal(PrincipalType.USER, "my_user")))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    private static Stream<Arguments> tableColumnOperationTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Set<String>>> methods = Stream.of(
                OpaAccessControl::checkCanSelectFromColumns,
                OpaAccessControl::checkCanUpdateTableColumns,
                OpaAccessControl::checkCanCreateViewWithSelectFromColumns);
        Stream<String> actionAndResource = Stream.of(
                "SelectFromColumns",
                "UpdateTableColumns",
                "CreateViewWithSelectFromColumns");
        return Streams.zip(actionAndResource, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#tableColumnOperationTestCases")
    public void testTableColumnOperations(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Set<String>> method)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        Set<String> columns = ImmutableSet.of("my_column");

        method.accept(authorizer, requestingSecurityContext, table, columns);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "tableName": "my_table",
                            "columns": ["my_column"]
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableColumnOperationFailureTestCases()
    {
        return createFailingTestCases(tableColumnOperationTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.opa.TestOpaAccessControl#tableColumnOperationFailureTestCases")
    public void testTableColumnOperationsFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Set<String>> method,
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        Set<String> columns = ImmutableSet.of("my_column");

        assertThatThrownBy(
                () -> method.accept(authorizer, requestingSecurityContext, table, columns))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    public void testCanSetCatalogSessionProperty()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, OK_RESPONSE));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        authorizer.checkCanSetCatalogSessionProperty(
                requestingSecurityContext, "my_catalog", "my_property");

        String expectedRequest = """
                {
                    "operation": "SetCatalogSessionProperty",
                    "resource": {
                        "catalogSessionProperty": {
                            "catalogName": "my_catalog",
                            "propertyName": "my_property"
                        }
                    }
                }
                """;
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanSetCatalogSessionPropertyFailure(
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, mockClient);

        assertThatThrownBy(
                () -> authorizer.checkCanSetCatalogSessionProperty(
                        requestingSecurityContext,
                        "my_catalog",
                        "my_property"))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    public void testFunctionResourceActions()
    {
        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("my_catalog", "my_schema", "my_routine_name");
        String baseRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "function": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "functionName": "my_routine_name"
                        }
                    }
                }""";
        assertAccessControlMethodBehaviour(
                new TestHelpers.ThrowingMethodWrapper(authorizer -> authorizer.checkCanExecuteProcedure(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("ExecuteProcedure")));
        assertAccessControlMethodBehaviour(
                new TestHelpers.ThrowingMethodWrapper(authorizer -> authorizer.checkCanCreateFunction(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("CreateFunction")));
        assertAccessControlMethodBehaviour(
                new TestHelpers.ThrowingMethodWrapper(authorizer -> authorizer.checkCanDropFunction(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("DropFunction")));
        assertAccessControlMethodBehaviour(
                new TestHelpers.ReturningMethodWrapper(authorizer -> authorizer.canExecuteFunction(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("ExecuteFunction")));
        assertAccessControlMethodBehaviour(
                new TestHelpers.ReturningMethodWrapper(authorizer -> authorizer.canCreateViewWithExecuteFunction(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("CreateViewWithExecuteFunction")));
    }

    @Test
    public void testCanExecuteTableProcedure()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        String expectedRequest = """
                {
                    "operation": "ExecuteTableProcedure",
                    "resource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "tableName": "my_table"
                        },
                        "function": {
                            "functionName": "my_procedure"
                        }
                    }
                }""";
        assertAccessControlMethodBehaviour(
                new TestHelpers.ThrowingMethodWrapper(authorizer -> authorizer.checkCanExecuteTableProcedure(TEST_SECURITY_CONTEXT, table, "my_procedure")),
                ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testRequestContextContentsWithKnownTrinoVersion()
    {
        testRequestContextContentsForGivenTrinoVersion(
                Optional.of(new TestingSystemAccessControlContext("12345.67890")),
                "12345.67890");
    }

    @Test
    public void testRequestContextContentsWithUnknownTrinoVersion()
    {
        testRequestContextContentsForGivenTrinoVersion(Optional.empty(), "UNKNOWN");
    }

    private void testRequestContextContentsForGivenTrinoVersion(Optional<SystemAccessControlFactory.SystemAccessControlContext> accessControlContext, String expectedTrinoVersion)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, request -> OK_RESPONSE);
        OpaAccessControl authorizer = (OpaAccessControl) OpaAccessControlFactory.create(
                ImmutableMap.of("opa.policy.uri", OPA_SERVER_URI.toString()),
                Optional.of(mockClient),
                accessControlContext);
        Identity sampleIdentityWithGroups = Identity.forUser("test_user").withGroups(ImmutableSet.of("some_group")).build();

        authorizer.checkCanExecuteQuery(sampleIdentityWithGroups);

        String expectedRequest = """
                {
                    "action": {
                        "operation": "ExecuteQuery"
                    },
                    "context": {
                        "identity": {
                            "user": "test_user",
                            "groups": ["some_group"]
                        },
                        "softwareStack": {
                            "trinoVersion": "%s"
                        }
                    }
                }""".formatted(expectedTrinoVersion);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input");
    }

    @Test
    public void testGetRowFiltersThrowsForIllegalResponse()
    {
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");
        assertAccessControlMethodThrowsForIllegalResponses(authorizer -> authorizer.getRowFilters(TEST_SECURITY_CONTEXT, tableName));

        // Also test a valid JSON response, but containing invalid fields for a row filters request
        String validJsonButIllegalSchemaResponseContents = """
                {
                    "result": ["some-expr"]
                }""";
        assertAccessControlMethodThrowsForResponse(
                authorizer -> authorizer.getRowFilters(TEST_SECURITY_CONTEXT, tableName),
                new MockResponse(validJsonButIllegalSchemaResponseContents, 200),
                OpaQueryException.class,
                "Failed to deserialize");
    }

    @Test
    public void testGetRowFilters()
    {
        // This example is a bit strange - an undefined policy would in most cases
        // result in an access denied situation. However, since this is row-level-filtering
        // we will accept this as meaning there are no known filters to be applied.
        testGetRowFilters("{}", ImmutableList.of());

        String noExpressionsResponse = """
                {
                    "result": []
                }""";
        testGetRowFilters(noExpressionsResponse, ImmutableList.of());

        String singleExpressionResponse = """
                {
                    "result": [
                        {"expression": "expr1"}
                    ]
                }""";
        testGetRowFilters(
                singleExpressionResponse,
                ImmutableList.of(new OpaViewExpression("expr1", Optional.empty())));

        String multipleExpressionsAndIdentitiesResponse = """
                {
                    "result": [
                        {"expression": "expr1"},
                        {"expression": "expr2", "identity": "expr2_identity"},
                        {"expression": "expr3", "identity": "expr3_identity"}
                    ]
                }""";
        testGetRowFilters(
                multipleExpressionsAndIdentitiesResponse,
                ImmutableList.<OpaViewExpression>builder()
                        .add(new OpaViewExpression("expr1", Optional.empty()))
                        .add(new OpaViewExpression("expr2", Optional.of("expr2_identity")))
                        .add(new OpaViewExpression("expr3", Optional.of("expr3_identity")))
                        .build());
    }

    private void testGetRowFilters(String responseContent, List<OpaViewExpression> expectedExpressions)
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(OPA_SERVER_ROW_FILTERING_URI, buildValidatingRequestHandler(TEST_IDENTITY, new MockResponse(responseContent, 200)));
        OpaAccessControl authorizer = createOpaAuthorizer(
                new TestHelpers.OpaConfigBuilder()
                        .withBasePolicy(OPA_SERVER_URI)
                        .withRowFiltersPolicy(OPA_SERVER_ROW_FILTERING_URI)
                        .buildConfig(),
                httpClient);
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");

        List<ViewExpression> result = authorizer.getRowFilters(TEST_SECURITY_CONTEXT, tableName);
        assertThat(result).allSatisfy(expression -> {
            assertThat(expression.getCatalog()).contains("some_catalog");
            assertThat(expression.getSchema()).contains("some_schema");
        });
        assertThat(result).map(
                viewExpression -> new OpaViewExpression(
                        viewExpression.getExpression(),
                        viewExpression.getSecurityIdentity()))
                .containsExactlyInAnyOrderElementsOf(expectedExpressions);

        String expectedRequest = """
                {
                    "operation": "GetRowFilters",
                    "resource": {
                        "table": {
                            "catalogName": "some_catalog",
                            "schemaName": "some_schema",
                            "tableName": "some_table"
                        }
                    }
                }""";
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), httpClient.getRequests(), "/input/action");
    }

    @Test
    public void testGetRowFiltersDoesNothingIfNotConfigured()
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(OPA_SERVER_ROW_FILTERING_URI, request -> {throw new AssertionError("Should not have been called");});
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, httpClient);
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");

        List<ViewExpression> result = authorizer.getRowFilters(TEST_SECURITY_CONTEXT, tableName);
        assertThat(result).isEmpty();
        assertThat(httpClient.getRequests()).isEmpty();
    }

    @Test
    public void testGetColumnMaskThrowsForIllegalResponse()
    {
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");
        assertAccessControlMethodThrowsForIllegalResponses(authorizer -> authorizer.getColumnMask(TEST_SECURITY_CONTEXT, tableName, "some_column", VarcharType.VARCHAR));

        // Also test a valid JSON response, but containing invalid fields for a row filters request
        String validJsonButIllegalSchemaResponseContents = """
                {
                    "result": {"expression": {"foo": "bar"}}
                }""";
        assertAccessControlMethodThrowsForResponse(
                authorizer -> authorizer.getColumnMask(TEST_SECURITY_CONTEXT, tableName, "some_column", VarcharType.VARCHAR),
                new MockResponse(validJsonButIllegalSchemaResponseContents, 200),
                OpaQueryException.class,
                "Failed to deserialize");
    }

    @Test
    public void testGetColumnMask()
    {
        // Similar note to the test for row level filtering:
        // This example is a bit strange - an undefined policy would in most cases
        // result in an access denied situation. However, since this is column masking,
        // we will accept this as meaning there are no masks to be applied.
        testGetColumnMask("{}", Optional.empty());

        String nullResponse = """
                {
                    "result": null
                }""";
        testGetColumnMask(nullResponse, Optional.empty());

        String expressionWithoutIdentityResponse = """
                {
                    "result": {"expression": "expr1"}
                }""";
        testGetColumnMask(
                expressionWithoutIdentityResponse,
                Optional.of(new OpaViewExpression("expr1", Optional.empty())));

        String expressionWithIdentityResponse = """
                {
                    "result": {"expression": "expr1", "identity": "some_identity"}
                }""";
        testGetColumnMask(
                expressionWithIdentityResponse,
                Optional.of(new OpaViewExpression("expr1", Optional.of("some_identity"))));
    }

    private void testGetColumnMask(String responseContent, Optional<OpaViewExpression> expectedExpression)
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(OPA_SERVER_COLUMN_MASK_URI, buildValidatingRequestHandler(TEST_IDENTITY, new MockResponse(responseContent, 200)));
        OpaAccessControl authorizer = createOpaAuthorizer(
                new TestHelpers.OpaConfigBuilder()
                        .withBasePolicy(OPA_SERVER_URI)
                        .withColumnMaskingPolicy(OPA_SERVER_COLUMN_MASK_URI)
                        .buildConfig(),
                httpClient);
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");

        Optional<ViewExpression> result = authorizer.getColumnMask(TEST_SECURITY_CONTEXT, tableName, "some_column", VarcharType.VARCHAR);

        assertThat(result.isEmpty()).isEqualTo(expectedExpression.isEmpty());
        assertThat(result.map(viewExpression -> {
            assertThat(viewExpression.getCatalog()).contains("some_catalog");
            assertThat(viewExpression.getSchema()).contains("some_schema");
            return new OpaViewExpression(viewExpression.getExpression(), viewExpression.getSecurityIdentity());
        })).isEqualTo(expectedExpression);

        String expectedRequest = """
                {
                    "operation": "GetColumnMask",
                    "resource": {
                        "column": {
                            "catalogName": "some_catalog",
                            "schemaName": "some_schema",
                            "tableName": "some_table",
                            "columnName": "some_column",
                            "columnType": "varchar"
                        }
                    }
                }""";
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), httpClient.getRequests(), "/input/action");
    }

    @Test
    public void testGetColumnMaskDoesNothingIfNotConfigured()
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(OPA_SERVER_COLUMN_MASK_URI, request -> {throw new AssertionError("Should not have been called");});
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, httpClient);
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");

        Optional<ViewExpression> result = authorizer.getColumnMask(TEST_SECURITY_CONTEXT, tableName, "some_column", VarcharType.VARCHAR);
        assertThat(result).isEmpty();
        assertThat(httpClient.getRequests()).isEmpty();
    }

    private static void assertAccessControlMethodBehaviour(MethodWrapper method, Set<String> expectedRequests)
    {
        InstrumentedHttpClient permissiveMockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(TEST_IDENTITY, OK_RESPONSE));
        InstrumentedHttpClient restrictiveMockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(TEST_IDENTITY, NO_ACCESS_RESPONSE));

        assertThat(method.isAccessAllowed(createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, permissiveMockClient))).isTrue();
        assertThat(method.isAccessAllowed(createOpaAuthorizer(OPA_CONFIG_WITH_ONLY_ALLOW, restrictiveMockClient))).isFalse();
        assertThat(permissiveMockClient.getRequests()).containsExactlyInAnyOrderElementsOf(restrictiveMockClient.getRequests());
        assertStringRequestsEqual(expectedRequests, permissiveMockClient.getRequests(), "/input/action");
        assertAccessControlMethodThrowsForIllegalResponses(method::isAccessAllowed);
    }

    private static void assertAccessControlMethodThrowsForIllegalResponses(Consumer<OpaAccessControl> methodToTest)
    {
        assertAccessControlMethodThrowsForResponse(methodToTest, UNDEFINED_RESPONSE, OpaQueryException.OpaServerError.PolicyNotFound.class, "did not return a value");
        assertAccessControlMethodThrowsForResponse(methodToTest, BAD_REQUEST_RESPONSE, OpaQueryException.OpaServerError.class, "returned status 400");
        assertAccessControlMethodThrowsForResponse(methodToTest, SERVER_ERROR_RESPONSE, OpaQueryException.OpaServerError.class, "returned status 500");
        assertAccessControlMethodThrowsForResponse(methodToTest, MALFORMED_RESPONSE, OpaQueryException.class, "Failed to deserialize");
    }

    private static void assertAccessControlMethodThrowsForResponse(
            Consumer<OpaAccessControl> methodToTest,
            MockResponse response,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(TEST_IDENTITY, response));
        OpaAccessControl authorizer = createOpaAuthorizer(
                new TestHelpers.OpaConfigBuilder()
                        .withBasePolicy(OPA_SERVER_URI)
                        .withRowFiltersPolicy(OPA_SERVER_URI)
                        .withColumnMaskingPolicy(OPA_SERVER_URI)
                        .buildConfig(),
                mockClient);

        assertThatThrownBy(() -> methodToTest.accept(authorizer))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }
}
