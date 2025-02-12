// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

db.getCollection('nested_columns').insertMany([
    {"_id": ObjectId("100000000000000000000101"), "name": "user_1", "age": 10, "address": "Shanghai", "parents": {"mother": "user_10", "father": "user_20"}},
    {"_id": ObjectId("100000000000000000000102"), "name": "user_2", "age": 20, "address": "Shanghai", "parents": {"mother": "user_11", "father": "user_21"}},
    {"_id": ObjectId("100000000000000000000103"), "name": "user_3", "age": 30, "address": "Shanghai", "parents": {"mother": "user_12", "father": "user_22"}},
    {"_id": ObjectId("100000000000000000000104"), "name": "user_4", "age": 40, "address": "Shanghai", "parents": {"mother": "user_13", "father": "user_23"}},
    {"_id": ObjectId("100000000000000000000105"), "name": "user_5", "age": 50, "address": "Shanghai", "parents": {"mother": "user_14", "father": "user_24"}},
    {"_id": ObjectId("100000000000000000000106"), "name": "user_6", "age": 60, "address": "Shanghai", "parents": {"mother": "user_15", "father": "user_25"}},
    {"_id": ObjectId("100000000000000000000107"), "name": "user_7", "age": 70, "address": "Shanghai", "parents": {"mother": "user_16", "father": "user_26"}},
    {"_id": ObjectId("100000000000000000000108"), "name": "user_8", "age": 80, "address": "Shanghai", "parents": {"mother": "user_17", "father": "user_27"}},
    {"_id": ObjectId("100000000000000000000109"), "name": "user_9", "age": 90, "address": "Shanghai", "parents": {"mother": "user_18"}}
]);
