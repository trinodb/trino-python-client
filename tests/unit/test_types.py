# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import math

from trino.client import TrinoStatus

"""
SQL query which generates the expected rows and columns:

SELECT false AS col_bool,
  cast(null AS BOOLEAN) AS col_bool_null,
  cast(127 AS TINYINT) AS col_tinyint,
  cast(-128 AS TINYINT) AS col_tinyint_min,
  cast(null AS TINYINT) AS col_tinyint_null,
  cast(32767 AS SMALLINT) AS col_smallint,
  cast(-32768 AS SMALLINT) AS col_smallint_min,
  cast(null AS SMALLINT) AS col_smallint_null,
  cast(2147483647 AS INTEGER) AS col_integer,
  cast(-2147483648 AS INTEGER) AS col_integer_min,
  cast(null AS INTEGER) AS col_integer_null,
  cast(9223372036854775807 AS BIGINT) AS col_bigint,
  cast(-9223372036854775808 AS BIGINT) AS col_bigint_min,
  cast(null AS BIGINT) AS col_bigint_null,
  cast(3.4028235E38 AS REAL) AS col_real,
  cast(1.4E-45 AS REAL) as col_real_min,
  cast('Infinity' AS REAL) AS col_real_inf,
  cast('-Infinity' AS REAL) AS col_real_ninf,
  cast('NaN' AS REAL) AS col_real_nan,
  cast(null AS REAL) AS col_real_null,
  cast(1.7976931348623157E308 AS DOUBLE) AS col_double,
  cast(4.9E-324 AS DOUBLE) as col_double_min,
  cast('Infinity' AS DOUBLE) AS col_double_inf,
  cast('-Infinity' AS DOUBLE) AS col_double_ninf,
  cast('NaN' AS DOUBLE) AS col_double_nan,
  cast(null AS DOUBLE) AS col_double_null,
  DECIMAL '10.3' AS col_decimal,
  cast(null AS DECIMAL) AS col_decimal_null,
  cast('0.123456789123456789' AS DECIMAL(18,18)) col_decimal18,
  cast(null AS DECIMAL(18,18)) AS col_decimal18_null,
  cast('10.3' AS DECIMAL(38,0)) col_decimal38,
  cast(null AS DECIMAL(38,0)) AS col_decimal38_null,
  'aaa' AS col_varchar,
  U&'Hello winter \2603 !' AS col_varchar_uni,
  cast(null AS VARCHAR) AS col_varchar_null,
  cast('bbb' AS VARCHAR(1)) AS col_varchar1,
  cast(null AS VARCHAR(1)) AS col_varchar1_null,
  cast('ccc' AS CHAR) AS col_char,
  cast(null AS CHAR) AS col_char_null,
  cast('ddd' AS CHAR(1)) AS col_char1,
  cast(null AS CHAR(1)) AS col_char1_null,
  X'65683F' AS col_varbinary,
  cast(null AS VARBINARY) AS col_varbinary_null,
  cast('{}' AS JSON) AS col_json,
  cast('null' AS JSON) AS col_json_null2,
  cast(null AS JSON) AS col_json_null,
  DATE '2001-08-22' AS col_date,
  cast(null AS DATE) AS col_date_null,
  TIME '01:23:45.123' AS col_time,
  cast(null AS TIME) AS col_time_null,
  cast('01:23:45' AS TIME(0)) AS col_time0,
  cast(null AS TIME(0)) AS col_time0_null,
  cast('01:23:45.123' AS TIME(3)) AS col_time3,
  cast(null AS TIME(3)) AS col_time3_null,
  cast('01:23:45.123456' AS TIME(6)) AS col_time6,
  cast(null AS TIME(6)) AS col_time6_null,
  cast('01:23:45.123456789' AS TIME(9)) AS col_time9,
  cast(null AS TIME(9)) AS col_time9_null,
  cast('01:23:45.123456789123' AS TIME(12)) AS col_time12,
  cast(null AS TIME(12)) AS col_time12_null,
  TIME '01:23:45.123 -08:00' AS col_timetz,
  cast(null AS TIME WITH TIME ZONE) AS col_timetz_null,
  cast('01:23:45 -08:00' AS TIME(0) WITH TIME ZONE) AS col_timetz0,
  cast(null AS TIME(0) WITH TIME ZONE) AS col_timetz0_null,
  cast('01:23:45.123 -08:00' AS TIME(3) WITH TIME ZONE) AS col_timetz3,
  cast(null AS TIME(3) WITH TIME ZONE) AS col_timetz3_null,
  cast('01:23:45.123456 -08:00' AS TIME(6) WITH TIME ZONE) AS col_timetz6,
  cast(null AS TIME(6) WITH TIME ZONE) AS col_timetz6_null,
  cast('01:23:45.123456789 -08:00' AS TIME(9) WITH TIME ZONE) AS col_timetz9,
  cast(null AS TIME(9) WITH TIME ZONE) AS col_timetz9_null,
  cast('01:23:45.123456789123 -08:00' AS TIME(12) WITH TIME ZONE) AS col_timetz12,
  cast(null AS TIME(12) WITH TIME ZONE) AS col_timetz12_null,
  TIMESTAMP '2001-08-22 01:23:45.123' AS col_ts,
  cast(null AS TIMESTAMP) AS col_ts_null,
  cast('2001-08-22 01:23:45' AS TIMESTAMP(0)) AS col_ts0,
  cast(null AS TIMESTAMP(0)) AS col_ts0_null,
  cast('2001-08-22 01:23:45.123' AS TIMESTAMP(3)) AS col_ts3,
  cast(null AS TIMESTAMP(3)) AS col_ts3_null,
  cast('2001-08-22 01:23:45.123456' AS TIMESTAMP(6)) AS col_ts6,
  cast(null AS TIMESTAMP(6)) AS col_ts6_null,
  cast('2001-08-22 01:23:45.123456789' AS TIMESTAMP(9)) AS col_ts9,
  cast(null AS TIMESTAMP(9)) AS col_ts9_null,
  cast('2001-08-22 01:23:45.123456789123' AS TIMESTAMP(12)) AS col_ts12,
  cast(null AS TIMESTAMP(12)) AS col_ts12_null,
  TIMESTAMP '2001-08-22 01:23:45.123 -08:00' AS col_tstz,
  cast(null AS TIMESTAMP WITH TIME ZONE) AS col_tstz_null,
  cast('2001-08-22 01:23:45 -08:00' AS TIMESTAMP(0) WITH TIME ZONE) AS col_tstz0,
  cast(null AS TIMESTAMP(0) WITH TIME ZONE) AS col_tstz0_null,
  cast('2001-08-22 01:23:45.123 -08:00' AS TIMESTAMP(3) WITH TIME ZONE) AS col_tstz3,
  cast(null AS TIMESTAMP(3) WITH TIME ZONE) AS col_tstz3_null,
  cast('2001-08-22 01:23:45.123456 -08:00' AS TIMESTAMP(6) WITH TIME ZONE) AS col_tstz6,
  cast(null AS TIMESTAMP(6) WITH TIME ZONE) AS col_tstz6_null,
  cast('2001-08-22 01:23:45.123456789 -08:00' AS TIMESTAMP(9) WITH TIME ZONE) AS col_tstz9,
  cast(null AS TIMESTAMP(9) WITH TIME ZONE) AS col_tstz9_null,
  cast('2001-08-22 01:23:45.123456789123 -08:00' AS TIMESTAMP(12) WITH TIME ZONE) AS col_tstz12,
  cast(null AS TIMESTAMP(12) WITH TIME ZONE) AS col_tstz12_null,
  INTERVAL '3' MONTH AS col_int_year,
  cast(null AS INTERVAL YEAR TO MONTH) AS col_int_year_null,
  INTERVAL '2' DAY AS col_int_day,
  cast(null AS INTERVAL DAY TO SECOND) AS col_int_day_null,
  ARRAY['a', 'b', null] AS col_array,
  cast(null AS ARRAY(VARCHAR)) AS col_array_null,
  MAP(ARRAY['a', 'b'], ARRAY[1, null]) AS col_map,
  cast(null AS MAP(VARCHAR, INTEGER)) AS col_map_null,
  cast(ROW(1, 2e0) AS ROW(x BIGINT, y DOUBLE)) AS col_row,
  cast(null AS ROW(x BIGINT, y DOUBLE)) AS col_row_null,
  IPADDRESS '2001:db8::1' AS col_ipaddr,
  cast(null AS IPADDRESS) AS col_ipaddr_null,
  UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59' AS col_uuid,
  cast(null AS UUID) AS col_uuid_null,
  approx_set(1) AS col_hll,
  cast(null AS HyperLogLog) AS col_hll_null,
  cast(approx_set(1) AS P4HyperLogLog) AS col_p4hll,
  cast(null AS P4HyperLogLog) AS col_p4hll_null,
  make_set_digest(1) AS col_setdigest,
  cast(null AS SetDigest) AS col_setdigest_null,
  qdigest_agg(1) AS col_qdigest,
  cast(null AS QDigest(BIGINT)) AS col_qdigest_null,
  tdigest_agg(1) AS col_tdigest,
  cast(null AS TDigest) AS col_tdigest_null
  """

columns = [
        {
            "name": "col_bool",
            "type": "boolean",
            "typeSignature": {
                "rawType": "boolean",
                "arguments": []
            }
        },
        {
            "name": "col_bool_null",
            "type": "boolean",
            "typeSignature": {
                "rawType": "boolean",
                "arguments": []
            }
        },
        {
            "name": "col_tinyint",
            "type": "tinyint",
            "typeSignature": {
                "rawType": "tinyint",
                "arguments": []
            }
        },
        {
            "name": "col_tinyint_min",
            "type": "tinyint",
            "typeSignature": {
                "rawType": "tinyint",
                "arguments": []
            }
        },
        {
            "name": "col_tinyint_null",
            "type": "tinyint",
            "typeSignature": {
                "rawType": "tinyint",
                "arguments": []
            }
        },
        {
            "name": "col_smallint",
            "type": "smallint",
            "typeSignature": {
                "rawType": "smallint",
                "arguments": []
            }
        },
        {
            "name": "col_smallint_min",
            "type": "smallint",
            "typeSignature": {
                "rawType": "smallint",
                "arguments": []
            }
        },
        {
            "name": "col_smallint_null",
            "type": "smallint",
            "typeSignature": {
                "rawType": "smallint",
                "arguments": []
            }
        },
        {
            "name": "col_integer",
            "type": "integer",
            "typeSignature": {
                "rawType": "integer",
                "arguments": []
            }
        },
        {
            "name": "col_integer_min",
            "type": "integer",
            "typeSignature": {
                "rawType": "integer",
                "arguments": []
            }
        },
        {
            "name": "col_integer_null",
            "type": "integer",
            "typeSignature": {
                "rawType": "integer",
                "arguments": []
            }
        },
        {
            "name": "col_bigint",
            "type": "bigint",
            "typeSignature": {
                "rawType": "bigint",
                "arguments": []
            }
        },
        {
            "name": "col_bigint_min",
            "type": "bigint",
            "typeSignature": {
                "rawType": "bigint",
                "arguments": []
            }
        },
        {
            "name": "col_bigint_null",
            "type": "bigint",
            "typeSignature": {
                "rawType": "bigint",
                "arguments": []
            }
        },
        {
            "name": "col_real",
            "type": "real",
            "typeSignature": {
                "rawType": "real",
                "arguments": []
            }
        },
        {
            "name": "col_real_min",
            "type": "real",
            "typeSignature": {
                "rawType": "real",
                "arguments": []
            }
        },
        {
            "name": "col_real_inf",
            "type": "real",
            "typeSignature": {
                "rawType": "real",
                "arguments": []
            }
        },
        {
            "name": "col_real_ninf",
            "type": "real",
            "typeSignature": {
                "rawType": "real",
                "arguments": []
            }
        },
        {
            "name": "col_real_nan",
            "type": "real",
            "typeSignature": {
                "rawType": "real",
                "arguments": []
            }
        },
        {
            "name": "col_real_null",
            "type": "real",
            "typeSignature": {
                "rawType": "real",
                "arguments": []
            }
        },
        {
            "name": "col_double",
            "type": "double",
            "typeSignature": {
                "rawType": "double",
                "arguments": []
            }
        },
        {
            "name": "col_double_min",
            "type": "double",
            "typeSignature": {
                "rawType": "double",
                "arguments": []
            }
        },
        {
            "name": "col_double_inf",
            "type": "double",
            "typeSignature": {
                "rawType": "double",
                "arguments": []
            }
        },
        {
            "name": "col_double_ninf",
            "type": "double",
            "typeSignature": {
                "rawType": "double",
                "arguments": []
            }
        },
        {
            "name": "col_double_nan",
            "type": "double",
            "typeSignature": {
                "rawType": "double",
                "arguments": []
            }
        },
        {
            "name": "col_double_null",
            "type": "double",
            "typeSignature": {
                "rawType": "double",
                "arguments": []
            }
        },
        {
            "name": "col_decimal",
            "type": "decimal(3, 1)",
            "typeSignature": {
                "rawType": "decimal",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    },
                    {
                        "kind": "LONG",
                        "value": 1
                    }
                ]
            }
        },
        {
            "name": "col_decimal_null",
            "type": "decimal(38, 0)",
            "typeSignature": {
                "rawType": "decimal",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 38
                    },
                    {
                        "kind": "LONG",
                        "value": 0
                    }
                ]
            }
        },
        {
            "name": "col_decimal18",
            "type": "decimal(18, 18)",
            "typeSignature": {
                "rawType": "decimal",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 18
                    },
                    {
                        "kind": "LONG",
                        "value": 18
                    }
                ]
            }
        },
        {
            "name": "col_decimal18_null",
            "type": "decimal(18, 18)",
            "typeSignature": {
                "rawType": "decimal",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 18
                    },
                    {
                        "kind": "LONG",
                        "value": 18
                    }
                ]
            }
        },
        {
            "name": "col_decimal38",
            "type": "decimal(38, 0)",
            "typeSignature": {
                "rawType": "decimal",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 38
                    },
                    {
                        "kind": "LONG",
                        "value": 0
                    }
                ]
            }
        },
        {
            "name": "col_decimal38_null",
            "type": "decimal(38, 0)",
            "typeSignature": {
                "rawType": "decimal",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 38
                    },
                    {
                        "kind": "LONG",
                        "value": 0
                    }
                ]
            }
        },
        {
            "name": "col_varchar",
            "type": "varchar(3)",
            "typeSignature": {
                "rawType": "varchar",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_varchar_uni",
            "type": "varchar(16)",
            "typeSignature": {
                "rawType": "varchar",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 16
                    }
                ]
            }
        },
        {
            "name": "col_varchar_null",
            "type": "varchar",
            "typeSignature": {
                "rawType": "varchar",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 2147483647
                    }
                ]
            }
        },
        {
            "name": "col_varchar1",
            "type": "varchar(1)",
            "typeSignature": {
                "rawType": "varchar",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 1
                    }
                ]
            }
        },
        {
            "name": "col_varchar1_null",
            "type": "varchar(1)",
            "typeSignature": {
                "rawType": "varchar",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 1
                    }
                ]
            }
        },
        {
            "name": "col_char",
            "type": "char(1)",
            "typeSignature": {
                "rawType": "char",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 1
                    }
                ]
            }
        },
        {
            "name": "col_char_null",
            "type": "char(1)",
            "typeSignature": {
                "rawType": "char",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 1
                    }
                ]
            }
        },
        {
            "name": "col_char1",
            "type": "char(1)",
            "typeSignature": {
                "rawType": "char",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 1
                    }
                ]
            }
        },
        {
            "name": "col_char1_null",
            "type": "char(1)",
            "typeSignature": {
                "rawType": "char",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 1
                    }
                ]
            }
        },
        {
            "name": "col_varbinary",
            "type": "varbinary",
            "typeSignature": {
                "rawType": "varbinary",
                "arguments": []
            }
        },
        {
            "name": "col_varbinary_null",
            "type": "varbinary",
            "typeSignature": {
                "rawType": "varbinary",
                "arguments": []
            }
        },
        {
            "name": "col_json",
            "type": "json",
            "typeSignature": {
                "rawType": "json",
                "arguments": []
            }
        },
        {
            "name": "col_json_null2",
            "type": "json",
            "typeSignature": {
                "rawType": "json",
                "arguments": []
            }
        },
        {
            "name": "col_json_null",
            "type": "json",
            "typeSignature": {
                "rawType": "json",
                "arguments": []
            }
        },
        {
            "name": "col_date",
            "type": "date",
            "typeSignature": {
                "rawType": "date",
                "arguments": []
            }
        },
        {
            "name": "col_date_null",
            "type": "date",
            "typeSignature": {
                "rawType": "date",
                "arguments": []
            }
        },
        {
            "name": "col_time",
            "type": "time(3)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_time_null",
            "type": "time(3)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_time0",
            "type": "time(0)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 0
                    }
                ]
            }
        },
        {
            "name": "col_time0_null",
            "type": "time(0)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 0
                    }
                ]
            }
        },
        {
            "name": "col_time3",
            "type": "time(3)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_time3_null",
            "type": "time(3)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_time6",
            "type": "time(6)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 6
                    }
                ]
            }
        },
        {
            "name": "col_time6_null",
            "type": "time(6)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 6
                    }
                ]
            }
        },
        {
            "name": "col_time9",
            "type": "time(9)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 9
                    }
                ]
            }
        },
        {
            "name": "col_time9_null",
            "type": "time(9)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 9
                    }
                ]
            }
        },
        {
            "name": "col_time12",
            "type": "time(12)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 12
                    }
                ]
            }
        },
        {
            "name": "col_time12_null",
            "type": "time(12)",
            "typeSignature": {
                "rawType": "time",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 12
                    }
                ]
            }
        },
        {
            "name": "col_timetz",
            "type": "time(3) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_timetz_null",
            "type": "time(3) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_timetz0",
            "type": "time(0) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 0
                    }
                ]
            }
        },
        {
            "name": "col_timetz0_null",
            "type": "time(0) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 0
                    }
                ]
            }
        },
        {
            "name": "col_timetz3",
            "type": "time(3) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_timetz3_null",
            "type": "time(3) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_timetz6",
            "type": "time(6) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 6
                    }
                ]
            }
        },
        {
            "name": "col_timetz6_null",
            "type": "time(6) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 6
                    }
                ]
            }
        },
        {
            "name": "col_timetz9",
            "type": "time(9) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 9
                    }
                ]
            }
        },
        {
            "name": "col_timetz9_null",
            "type": "time(9) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 9
                    }
                ]
            }
        },
        {
            "name": "col_timetz12",
            "type": "time(12) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 12
                    }
                ]
            }
        },
        {
            "name": "col_timetz12_null",
            "type": "time(12) with time zone",
            "typeSignature": {
                "rawType": "time with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 12
                    }
                ]
            }
        },
        {
            "name": "col_ts",
            "type": "timestamp(3)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_ts_null",
            "type": "timestamp(3)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_ts0",
            "type": "timestamp(0)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 0
                    }
                ]
            }
        },
        {
            "name": "col_ts0_null",
            "type": "timestamp(0)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 0
                    }
                ]
            }
        },
        {
            "name": "col_ts3",
            "type": "timestamp(3)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_ts3_null",
            "type": "timestamp(3)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_ts6",
            "type": "timestamp(6)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 6
                    }
                ]
            }
        },
        {
            "name": "col_ts6_null",
            "type": "timestamp(6)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 6
                    }
                ]
            }
        },
        {
            "name": "col_ts9",
            "type": "timestamp(9)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 9
                    }
                ]
            }
        },
        {
            "name": "col_ts9_null",
            "type": "timestamp(9)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 9
                    }
                ]
            }
        },
        {
            "name": "col_ts12",
            "type": "timestamp(12)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 12
                    }
                ]
            }
        },
        {
            "name": "col_ts12_null",
            "type": "timestamp(12)",
            "typeSignature": {
                "rawType": "timestamp",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 12
                    }
                ]
            }
        },
        {
            "name": "col_tstz",
            "type": "timestamp(3) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_tstz_null",
            "type": "timestamp(3) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_tstz0",
            "type": "timestamp(0) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 0
                    }
                ]
            }
        },
        {
            "name": "col_tstz0_null",
            "type": "timestamp(0) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 0
                    }
                ]
            }
        },
        {
            "name": "col_tstz3",
            "type": "timestamp(3) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_tstz3_null",
            "type": "timestamp(3) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 3
                    }
                ]
            }
        },
        {
            "name": "col_tstz6",
            "type": "timestamp(6) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 6
                    }
                ]
            }
        },
        {
            "name": "col_tstz6_null",
            "type": "timestamp(6) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 6
                    }
                ]
            }
        },
        {
            "name": "col_tstz9",
            "type": "timestamp(9) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 9
                    }
                ]
            }
        },
        {
            "name": "col_tstz9_null",
            "type": "timestamp(9) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 9
                    }
                ]
            }
        },
        {
            "name": "col_tstz12",
            "type": "timestamp(12) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 12
                    }
                ]
            }
        },
        {
            "name": "col_tstz12_null",
            "type": "timestamp(12) with time zone",
            "typeSignature": {
                "rawType": "timestamp with time zone",
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 12
                    }
                ]
            }
        },
        {
            "name": "col_int_year",
            "type": "INTERVAL YEAR TO MONTH",
            "typeSignature": {
                "rawType": "interval year to month",
                "arguments": []
            }
        },
        {
            "name": "col_int_year_null",
            "type": "INTERVAL YEAR TO MONTH",
            "typeSignature": {
                "rawType": "interval year to month",
                "arguments": []
            }
        },
        {
            "name": "col_int_day",
            "type": "INTERVAL DAY TO SECOND",
            "typeSignature": {
                "rawType": "interval day to second",
                "arguments": []
            }
        },
        {
            "name": "col_int_day_null",
            "type": "INTERVAL DAY TO SECOND",
            "typeSignature": {
                "rawType": "interval day to second",
                "arguments": []
            }
        },
        {
            "name": "col_array",
            "type": "array(varchar(1))",
            "typeSignature": {
                "rawType": "array",
                "arguments": [
                    {
                        "kind": "TYPE",
                        "value": {
                            "rawType": "varchar",
                            "arguments": [
                                {
                                    "kind": "LONG",
                                    "value": 1
                                }
                            ]
                        }
                    }
                ]
            }
        },
        {
            "name": "col_array_null",
            "type": "array(varchar)",
            "typeSignature": {
                "rawType": "array",
                "arguments": [
                    {
                        "kind": "TYPE",
                        "value": {
                            "rawType": "varchar",
                            "arguments": [
                                {
                                    "kind": "LONG",
                                    "value": 2147483647
                                }
                            ]
                        }
                    }
                ]
            }
        },
        {
            "name": "col_map",
            "type": "map(varchar(1), integer)",
            "typeSignature": {
                "rawType": "map",
                "arguments": [
                    {
                        "kind": "TYPE",
                        "value": {
                            "rawType": "varchar",
                            "arguments": [
                                {
                                    "kind": "LONG",
                                    "value": 1
                                }
                            ]
                        }
                    },
                    {
                        "kind": "TYPE",
                        "value": {
                            "rawType": "integer",
                            "arguments": []
                        }
                    }
                ]
            }
        },
        {
            "name": "col_map_null",
            "type": "map(varchar, integer)",
            "typeSignature": {
                "rawType": "map",
                "arguments": [
                    {
                        "kind": "TYPE",
                        "value": {
                            "rawType": "varchar",
                            "arguments": [
                                {
                                    "kind": "LONG",
                                    "value": 2147483647
                                }
                            ]
                        }
                    },
                    {
                        "kind": "TYPE",
                        "value": {
                            "rawType": "integer",
                            "arguments": []
                        }
                    }
                ]
            }
        },
        {
            "name": "col_row",
            "type": "row(x bigint, y double)",
            "typeSignature": {
                "rawType": "row",
                "arguments": [
                    {
                        "kind": "NAMED_TYPE",
                        "value": {
                            "fieldName": {
                                "name": "x"
                            },
                            "typeSignature": {
                                "rawType": "bigint",
                                "arguments": []
                            }
                        }
                    },
                    {
                        "kind": "NAMED_TYPE",
                        "value": {
                            "fieldName": {
                                "name": "y"
                            },
                            "typeSignature": {
                                "rawType": "double",
                                "arguments": []
                            }
                        }
                    }
                ]
            }
        },
        {
            "name": "col_row_null",
            "type": "row(x bigint, y double)",
            "typeSignature": {
                "rawType": "row",
                "arguments": [
                    {
                        "kind": "NAMED_TYPE",
                        "value": {
                            "fieldName": {
                                "name": "x"
                            },
                            "typeSignature": {
                                "rawType": "bigint",
                                "arguments": []
                            }
                        }
                    },
                    {
                        "kind": "NAMED_TYPE",
                        "value": {
                            "fieldName": {
                                "name": "y"
                            },
                            "typeSignature": {
                                "rawType": "double",
                                "arguments": []
                            }
                        }
                    }
                ]
            }
        },
        {
            "name": "col_ipaddr",
            "type": "ipaddress",
            "typeSignature": {
                "rawType": "ipaddress",
                "arguments": []
            }
        },
        {
            "name": "col_ipaddr_null",
            "type": "ipaddress",
            "typeSignature": {
                "rawType": "ipaddress",
                "arguments": []
            }
        },
        {
            "name": "col_uuid",
            "type": "uuid",
            "typeSignature": {
                "rawType": "uuid",
                "arguments": []
            }
        },
        {
            "name": "col_uuid_null",
            "type": "uuid",
            "typeSignature": {
                "rawType": "uuid",
                "arguments": []
            }
        },
        {
            "name": "col_hll",
            "type": "HyperLogLog",
            "typeSignature": {
                "rawType": "HyperLogLog",
                "arguments": []
            }
        },
        {
            "name": "col_hll_null",
            "type": "HyperLogLog",
            "typeSignature": {
                "rawType": "HyperLogLog",
                "arguments": []
            }
        },
        {
            "name": "col_p4hll",
            "type": "P4HyperLogLog",
            "typeSignature": {
                "rawType": "P4HyperLogLog",
                "arguments": []
            }
        },
        {
            "name": "col_p4hll_null",
            "type": "P4HyperLogLog",
            "typeSignature": {
                "rawType": "P4HyperLogLog",
                "arguments": []
            }
        },
        {
            "name": "col_setdigest",
            "type": "SetDigest",
            "typeSignature": {
                "rawType": "SetDigest",
                "arguments": []
            }
        },
        {
            "name": "col_setdigest_null",
            "type": "SetDigest",
            "typeSignature": {
                "rawType": "SetDigest",
                "arguments": []
            }
        },
        {
            "name": "col_qdigest",
            "type": "qdigest(bigint)",
            "typeSignature": {
                "rawType": "qdigest",
                "arguments": [
                    {
                        "kind": "TYPE",
                        "value": {
                            "rawType": "bigint",
                            "arguments": []
                        }
                    }
                ]
            }
        },
        {
            "name": "col_qdigest_null",
            "type": "qdigest(bigint)",
            "typeSignature": {
                "rawType": "qdigest",
                "arguments": [
                    {
                        "kind": "TYPE",
                        "value": {
                            "rawType": "bigint",
                            "arguments": []
                        }
                    }
                ]
            }
        },
        {
            "name": "col_tdigest",
            "type": "tdigest",
            "typeSignature": {
                "rawType": "tdigest",
                "arguments": []
            }
        },
        {
            "name": "col_tdigest_null",
            "type": "tdigest",
            "typeSignature": {
                "rawType": "tdigest",
                "arguments": []
            }
        }
    ]

rows = [
        [
            False,
            None,
            127,
            -128,
            None,
            32767,
            -32768,
            None,
            2147483647,
            -2147483648,
            None,
            9223372036854775807,
            -9223372036854775808,
            None,
            3.4028235E38,
            1.4E-45,
            "Infinity",
            "-Infinity",
            "NaN",
            None,
            1.7976931348623157E308,
            4.9E-324,
            "Infinity",
            "-Infinity",
            "NaN",
            None,
            "10.3",
            None,
            "0.123456789123456789",
            None,
            "10",
            None,
            "aaa",
            "Hello winter °3 !",
            None,
            "b",
            None,
            "c",
            None,
            "d",
            None,
            "ZWg/",
            None,
            "\"{}\"",
            "\"null\"",
            None,
            "2001-08-22",
            None,
            "01:23:45.123",
            None,
            "01:23:45",
            None,
            "01:23:45.123",
            None,
            "01:23:45.123456",
            None,
            "01:23:45.123456789",
            None,
            "01:23:45.123456789123",
            None,
            "01:23:45.123-08:00",
            None,
            "01:23:45-08:00",
            None,
            "01:23:45.123-08:00",
            None,
            "01:23:45.123456-08:00",
            None,
            "01:23:45.123456789-08:00",
            None,
            "01:23:45.123456789123-08:00",
            None,
            "2001-08-22 01:23:45.123",
            None,
            "2001-08-22 01:23:45",
            None,
            "2001-08-22 01:23:45.123",
            None,
            "2001-08-22 01:23:45.123456",
            None,
            "2001-08-22 01:23:45.123456789",
            None,
            "2001-08-22 01:23:45.123456789123",
            None,
            "2001-08-22 01:23:45.123 -08:00",
            None,
            "2001-08-22 01:23:45 -08:00",
            None,
            "2001-08-22 01:23:45.123 -08:00",
            None,
            "2001-08-22 01:23:45.123456 -08:00",
            None,
            "2001-08-22 01:23:45.123456789 -08:00",
            None,
            "2001-08-22 01:23:45.123456789123 -08:00",
            None,
            "0-3",
            None,
            "2 00:00:00.000",
            None,
            [
                "a",
                "b",
                None
            ],
            None,
            {
                "a": 1,
                "b": None
            },
            None,
            [
                1,
                2.0
            ],
            None,
            "2001:db8::1",
            None,
            "12151fd2-7586-11e9-8f9e-2a86e4085a59",
            None,
            "AgwBAIADRAA=",
            None,
            "AwwAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
            None,
            "AQgAAAACCwEAgANEAAAgAAABAAAASsQF+7cDRAABAA==",
            None,
            "AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAAAAPA/AQAAAAAAAIA=",
            None,
            "AAAAAAAAAPA/AAAAAAAA8D8AAAAAAABZQAAAAAAAAPA/AQAAAAAAAAAAAPA/AAAAAAAA8D8=",
            None
        ]
    ]


EXPECTED_RESPONSE = [
    False,
    None,
    127,
    -128,
    None,
    32767,
    -32768,
    None,
    2147483647,
    -2147483648,
    None,
    9223372036854775807,
    -9223372036854775808,
    None,
    3.4028235e+38,
    1.4e-45,
    math.inf,
    -math.inf,
    math.nan,
    None,
    1.7976931348623157e+308,
    5e-324,
    math.inf,
    -math.inf,
    math.nan,
    None,
    10.3,
    None,
    0.12345678912345678,
    None,
    10.0,
    None,
    'aaa',
    'Hello winter \2603 !',
    None,
    'b',
    None,
    'c',
    None,
    'd',
    None,
    'ZWg/',
    None,
    {},
    None,
    None,
    datetime.date(2001, 8, 22),
    None,
    datetime.time(1, 23, 45, 123000),
    None,
    datetime.time(1, 23, 45),
    None,
    datetime.time(1, 23, 45, 123000),
    None,
    datetime.time(1, 23, 45, 123456),
    None,
    datetime.time(1, 23, 45, 123456),
    None,
    datetime.time(1, 23, 45, 123456),
    None,
    datetime.time(1, 23, 45, 123000),
    None,
    datetime.time(1, 23, 45),
    None,
    datetime.time(1, 23, 45, 123000),
    None,
    datetime.time(1, 23, 45, 123456),
    None,
    datetime.time(1, 23, 45, 123456),
    None,
    datetime.time(1, 23, 45, 123456),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45, 123000),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45, 123000),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45, 123456),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45, 189000),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45, 189123),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45, 123000, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=57600))),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=57600))),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45, 123000, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=57600))),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45, 123456, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=57600))),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45, 189000, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=57600))),
    None,
    datetime.datetime(2001, 8, 22, 1, 23, 45, 189123, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=57600))),
    None,
    '0-3',
    None,
    '2 00:00:00.000',
    None,
    ['a', 'b', None],
    None,
    {'a': 1, 'b': None},
    None,
    [1, 2.0],
    None,
    '2001:db8::1',
    None,
    '12151fd2-7586-11e9-8f9e-2a86e4085a59',
    None,
    'AgwBAIADRAA=',
    None,
    'AwwAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==',
    None,
    'AQgAAAACCwEAgANEAAAgAAABAAAASsQF+7cDRAABAA==',
    None,
    'AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAAAAPA/AQAAAAAAAIA=',
    None,
    'AAAAAAAAAPA/AAAAAAAA8D8AAAAAAABZQAAAAAAAAPA/AQAAAAAAAAAAAPA/AAAAAAAA8D8=',
    None
]

def test_types():
    t = TrinoStatus(id="id", stats={}, warnings=[], info_uri="", next_uri="", update_type="", rows=rows, columns=columns)

    assert len(t.rows) == 1
    result = t.rows[0]

    assert len(result) == len(EXPECTED_RESPONSE)

    for i in range(len(result)):
        lval = result[i]
        rval = EXPECTED_RESPONSE[i]

        if lval != rval:
            if type(lval) == float and math.isnan(lval) and type(rval) == float and math.isnan(rval):
                continue

            assert rval == lval
