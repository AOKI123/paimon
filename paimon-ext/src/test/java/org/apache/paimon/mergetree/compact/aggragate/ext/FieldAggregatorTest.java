/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.paimon.mergetree.compact.aggregate.ext;

import com.starrocks.types.BitmapValue;

/** test whether {@link FieldAggregator}' subclasses behaviors are expected. */
public class FieldAggregatorTest {

    @Test
    public void testFieldStarRocksBitmapAgg() throws IOException {
        FieldStarRocksBitmapAgg agg =
                new FieldStarRocksBitmapAggFactory().create(DataTypes.VARBINARY(20), null, null);
        testAgg(agg);
    }

    @Test
    public void testCustomAgg() throws IOException {
        FieldAggregator agg =
                FieldAggregatorFactory.create(
                        DataTypes.STRING(),
                        "bitmap",
                        "to_bitmap",
                        CoreOptions.fromMap(new HashMap<>()));

        testAgg(agg);
    }

    private void testAgg(FieldAggregator agg) {
        byte[] inputVal =  BitmapValue.bitmapToBytes(BitmapValue(1L));
        byte[] acc1 = BitmapValue.bitmapToBytes(BitmapValue(2L).add(3L));
        byte[] acc2 = BitmapValue.bitmapToBytes(BitmapValue(1L).add(2L).add(3L));

        assertThat(agg.agg(null, null)).isNull();

        byte[] result1 = (byte[]) agg.agg(null, inputVal);
        assertThat(inputVal).isEqualTo(result1);

        byte[] result2 = (byte[]) agg.agg(acc1, null);
        assertThat(result2).isEqualTo(acc1);

        byte[] result3 = (byte[]) agg.agg(acc1, inputVal);
        assertThat(result3).isEqualTo(acc2);

        byte[] result4 = (byte[]) agg.agg(acc2, inputVal);
        assertThat(result4).isEqualTo(acc2);

        assertThat(BitmapValue.bitmapFromBytes(result4).cardinality()).isEqualTo(3L);
    }
}