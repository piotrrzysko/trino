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
package io.trino.sql.query;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLag
{
    @Test
    public void testNullOffset()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.query("""
                    SELECT lag(v, null) OVER (ORDER BY k)
                    FROM (VALUES (1, 10), (2, 20)) t(k, v)
                    """))
                    .failure().hasMessageMatching("Offset must not be null");
        }
    }

    @Test
    public void testWindowFrame()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.query("""
                        WITH orders (custkey, orderdate, totalprice)\s
                        AS
                        (
                        VALUES
                        ('cust_1', DATE '2020-10-10', 100),
                        ('cust_2', DATE '2020-10-10', 15),
                        ('cust_1', DATE '2020-10-15', 200),
                        ('cust_1', DATE '2020-10-16', 240),
                        ('cust_2', DATE '2020-12-20', 25),
                        ('cust_1', DATE '2020-12-25', 140),
                        ('cust_2', DATE '2021-01-01', 5)
                    )
                    SELECT *,
                           avg(totalprice) OVER (
                                                 PARTITION BY custkey
                                                 ORDER BY orderdate
                                                 RANGE BETWEEN INTERVAL '1' MONTH PRECEDING AND CURRENT ROW) AS past_month_avg,
                           lag(totalprice) OVER ( PARTITION BY custkey
                                                  ORDER BY orderdate
                                                  RANGE BETWEEN INTERVAL '1' MONTH PRECEDING AND CURRENT ROW) AS previous_total_price_within_last_month
                    FROM orders
                    """))
                    .failure().hasMessageContaining("Cannot specify window frame for lag function");
        }
    }
}
