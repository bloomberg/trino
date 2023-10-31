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
package io.trino.plugin.resourcegroups.db;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.h2.jdbcx.JdbcDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import static java.util.Objects.requireNonNull;

public class H2DaoProvider
        implements Provider<ResourceGroupsDao>
{
    private final H2ResourceGroupsDao dao;

    @Inject
    public H2DaoProvider(DbResourceGroupConfig config)
    {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(requireNonNull(config.getConfigDbUrl(), "resource-groups.config-db-url is null"));
        // TODO: this should use onDemand()
        this.dao = Jdbi.create(ds)
                .installPlugin(new SqlObjectPlugin())
                .open()
                .attach(H2ResourceGroupsDao.class);
    }

    @Override
    public H2ResourceGroupsDao get()
    {
        return dao;
    }
}
