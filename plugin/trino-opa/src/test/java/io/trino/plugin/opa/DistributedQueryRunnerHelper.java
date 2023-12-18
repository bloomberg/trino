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

import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class DistributedQueryRunnerHelper
{
    private final DistributedQueryRunner runner;

    private DistributedQueryRunnerHelper(DistributedQueryRunner runner)
    {
        this.runner = runner;
    }

    public static DistributedQueryRunnerHelper withOpaConfig(Map<String, String> opaConfig)
            throws Exception
    {
        return new DistributedQueryRunnerHelper(
                DistributedQueryRunner.builder(testSessionBuilder().build())
                        .setSystemAccessControl(new OpaAccessControlFactory().create(opaConfig))
                        .setNodeCount(1)
                        .build());
    }

    public Set<String> querySetOfStrings(String user, String query)
    {
        return querySetOfStrings(userSession(user), query);
    }

    public Set<String> querySetOfStrings(Session session, String query)
    {
        return runner.execute(session, query).getMaterializedRows().stream().map(row -> row.getField(0) == null ? "<NULL>" : row.getField(0).toString()).collect(toImmutableSet());
    }

    public DistributedQueryRunner getBaseQueryRunner()
    {
        return this.runner;
    }

    public void teardown()
    {
        if (this.runner != null) {
            this.runner.close();
        }
    }

    private static Session userSession(String user)
    {
        return testSessionBuilder().setIdentity(Identity.ofUser(user)).build();
    }
}
