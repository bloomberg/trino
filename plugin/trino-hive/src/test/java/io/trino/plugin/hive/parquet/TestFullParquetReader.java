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
package io.trino.plugin.hive.parquet;

import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// Failing on multiple threads because of org.apache.hadoop.hive.ql.io.parquet.write.ParquetRecordWriterWrapper
// uses a single record writer across all threads.
// For example org.apache.parquet.column.values.factory.DefaultValuesWriterFactory#DEFAULT_V1_WRITER_FACTORY is shared mutable state.
@Execution(SAME_THREAD)
public class TestFullParquetReader
        extends AbstractTestParquetReader
{
    public TestFullParquetReader()
    {
        super(ParquetTester.fullParquetTester());
    }
}
