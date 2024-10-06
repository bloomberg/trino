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
package io.trino.plugin.resourcegroups;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.resourcegroups.SelectorResourceEstimate.Range;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
import io.trino.spi.session.ResourceEstimates;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.Unit.TERABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStaticSelector
{
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty());

    @Test
    public void testUserRegex()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");
        StaticSelector selector = new StaticSelector(
                Optional.of(Pattern.compile("user.*")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));
        assertThat(selector.match(newSelectionCriteria("userA", null, ImmutableSet.of("tag1"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(resourceGroupId));
        assertThat(selector.match(newSelectionCriteria("userB", "source", ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(resourceGroupId));
        assertThat(selector.match(newSelectionCriteria("A.user", null, ImmutableSet.of("tag1"), EMPTY_RESOURCE_ESTIMATES))).isEqualTo(Optional.empty());
    }

    @Test
    public void testSourceRegex()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");
        StaticSelector selector = new StaticSelector(
                Optional.empty(),
                Optional.empty(),
                Optional.of(Pattern.compile(".*source.*")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));
        assertThat(selector.match(newSelectionCriteria("userA", null, ImmutableSet.of("tag1"), EMPTY_RESOURCE_ESTIMATES))).isEqualTo(Optional.empty());
        assertThat(selector.match(newSelectionCriteria("userB", "source", ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(resourceGroupId));
        assertThat(selector.match(newSelectionCriteria("A.user", "a source b", ImmutableSet.of("tag1"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(resourceGroupId));
    }

    @Test
    public void testClientTags()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");
        StaticSelector selector = new StaticSelector(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag1", "tag2")),
                Optional.empty(),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));
        assertThat(selector.match(newSelectionCriteria("userA", null, ImmutableSet.of("tag1", "tag2"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(resourceGroupId));
        assertThat(selector.match(newSelectionCriteria("userB", "source", ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES))).isEqualTo(Optional.empty());
        assertThat(selector.match(newSelectionCriteria("A.user", "a source b", ImmutableSet.of("tag1"), EMPTY_RESOURCE_ESTIMATES))).isEqualTo(Optional.empty());
        assertThat(selector.match(newSelectionCriteria("A.user", "a source b", ImmutableSet.of("tag1", "tag2", "tag3"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(resourceGroupId));
    }

    @Test
    public void testSelectorResourceEstimate()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");

        StaticSelector smallQuerySelector = new StaticSelector(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new SelectorResourceEstimate(
                        Optional.of(new Range<>(
                                Optional.empty(),
                                Optional.of(new Duration(5, MINUTES)))),
                        Optional.empty(),
                        Optional.of(new Range<>(
                                Optional.empty(),
                                Optional.of(DataSize.valueOf("500MB")))))),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));

        assertThat(smallQuerySelector.match(
                        newSelectionCriteria(
                                "userA",
                                null,
                                ImmutableSet.of("tag1", "tag2"),
                                new ResourceEstimates(
                                        Optional.of(java.time.Duration.ofMinutes(4)),
                                        Optional.empty(),
                                        Optional.of(DataSize.of(400, MEGABYTE).toBytes()))))
                .map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(resourceGroupId));

        assertThat(smallQuerySelector.match(
                        newSelectionCriteria(
                                "A.user",
                                "a source b",
                                ImmutableSet.of("tag1"),
                                new ResourceEstimates(
                                        Optional.of(java.time.Duration.ofMinutes(4)),
                                        Optional.empty(),
                                        Optional.of(DataSize.of(600, MEGABYTE).toBytes()))))
                .map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.empty());

        assertThat(smallQuerySelector.match(
                        newSelectionCriteria(
                                "userB",
                                "source",
                                ImmutableSet.of(),
                                new ResourceEstimates(
                                        Optional.of(java.time.Duration.ofMinutes(4)),
                                        Optional.empty(),
                                        Optional.empty())))
                .map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.empty());

        StaticSelector largeQuerySelector = new StaticSelector(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new SelectorResourceEstimate(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Range<>(
                                Optional.of(DataSize.valueOf("5TB")),
                                Optional.empty())))),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));

        assertThat(largeQuerySelector.match(
                        newSelectionCriteria(
                                "userA",
                                null,
                                ImmutableSet.of("tag1", "tag2"),
                                new ResourceEstimates(
                                        Optional.of(java.time.Duration.ofHours(100)),
                                        Optional.empty(),
                                        Optional.of(DataSize.of(4, TERABYTE).toBytes()))))
                .map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.empty());

        assertThat(largeQuerySelector.match(
                        newSelectionCriteria(
                                "A.user",
                                "a source b",
                                ImmutableSet.of("tag1"),
                                new ResourceEstimates(
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.of(DataSize.of(6, TERABYTE).toBytes()))))
                .map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(resourceGroupId));

        assertThat(largeQuerySelector.match(
                        newSelectionCriteria(
                                "userB",
                                "source",
                                ImmutableSet.of(),
                                new ResourceEstimates(
                                        Optional.of(java.time.Duration.ofSeconds(1)),
                                        Optional.of(java.time.Duration.ofSeconds(1)),
                                        Optional.of(DataSize.of(6, TERABYTE).toBytes()))))
                .map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(resourceGroupId));
    }

    private SelectionCriteria newSelectionCriteria(String user, String source, Set<String> tags, ResourceEstimates resourceEstimates)
    {
        return new SelectionCriteria(true, user, ImmutableSet.of(), Optional.ofNullable(source), tags, resourceEstimates, Optional.empty());
    }
}
