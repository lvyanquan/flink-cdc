-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE IF NOT EXISTS
  `sink_catalog_foo`.`sink_database_bar`.`sink_table_baz`
WITH (
  'enableTypeNormalization' = 'true'
)
AS TABLE
  `source_table_baz`
/*+ `OPTIONS`(
  'server-id' = '5611-5620',
  'jdbc.properties.tinyInt1isBit' = 'false',
  'jdbc.properties.transformedBitIsBoolean' = 'false'
) */
