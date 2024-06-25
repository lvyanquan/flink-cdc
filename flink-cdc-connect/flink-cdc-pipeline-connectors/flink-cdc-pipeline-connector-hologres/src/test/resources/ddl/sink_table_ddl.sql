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

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  column_type_test
-- ----------------------------------------------------------------------------------------------------------------


BEGIN;
CREATE TABLE IF NOT EXISTS $SCHEMA$.$TABLE$ (
    a char,
    b character varying,
    c text,
    d boolean,
    e numeric(6, 2),
    f smallint,
    g smallint,
    h integer NOT NULL,
    i bigint,
    j real,
    k double precision,
    l date,
    m time,
    n timestamp without time zone,
    o timestamp with time zone,
    p timestamp with time zone,
    q boolean[],
    r integer[],
    s bigint[],
    t real[],
    u double precision[],
    v varchar[],
    w varchar[],
   PRIMARY KEY (h)
);
END;