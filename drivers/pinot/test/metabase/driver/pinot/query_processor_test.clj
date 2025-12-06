;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns metabase.driver.pinot.query-processor-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [metabase.driver.pinot.query-processor :as qp]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.query-processor.store :as qp.store]))

(def metadata-provider
  {:tables {1 {:id 1 :name "events"}}
   :fields {1 {:id 1 :name "id"}
            2 {:id 2 :name "price"}}})

(def redefine-metadata!
  {:qp.store/metadata-provider (constantly metadata-provider)
   :lib.metadata/table (fn [provider table-id]
                         (get-in provider [:tables table-id]))
   :lib.metadata/field (fn [provider field-id]
                         (get-in provider [:fields field-id]))})

(deftest mbql-to-native-builds-complete-sql
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :settings {:timezone "UTC"}
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :breakout [[:field 1 nil]]
                         :aggregation [[:aggregation [:count [:field 1 nil]] {:name "total"}]]
                         :filter [:and [:= [:field 1 nil] 10] [:> [:field 2 nil] 0]]
                         :order-by [[:asc [:field 1 nil]]]
                         :limit 10
                         :page 2}}
          result (qp/mbql->native query)
          sql (get-in result [:query :sql])]
      (is (true? (:mbql? result)))
      (is (= ["\"id\""] (get-in result [:query :columns])))
      (is (= ["\"id\""] (get-in result [:query :group-by])))
      (is (= ["COUNT(*) AS total"] (get-in result [:query :aggregations])))
      (is (= "(\"id\" = 10 AND \"price\" > 0)" (get-in result [:query :where])))
      (is (= ["\"id\" asc"] (get-in result [:query :order-by])))
      (is (= 10 (get-in result [:query :limit])))
      (is (= 20 (get-in result [:query :offset])))
      (is (= "events" (get-in result [:query :dataSource])))
      (is (= "SELECT \"id\", COUNT(*) AS total FROM events WHERE (\"id\" = 10 AND \"price\" > 0) GROUP BY \"id\" ORDER BY \"id\" asc LIMIT 20, 10"
             sql)))))

(deftest filters-handle-between-and-not
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :filter [:not [:between [:field 1 nil] 1 10]]}}
          result (qp/mbql->native query)]
      (is (= "NOT \"id\" BETWEEN 1 AND 10"
             (get-in result [:query :where])))
      (is (str/includes? (get-in result [:query :sql]) "NOT \"id\" BETWEEN 1 AND 10")))))

(deftest filters-handle-comparison-operators
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :filter [:or [:>= [:field 1 nil] 100]
                                  [:<= [:field 2 nil] 50]
                                  [:!= [:field 1 nil] 0]]}}
          sql (get-in (qp/mbql->native query) [:query :sql])]
      (is (str/includes? sql "\"id\" >= 100"))
      (is (str/includes? sql "\"price\" <= 50"))
      (is (str/includes? sql "\"id\" != 0")))))

(deftest aggregations-generate-expected-sql
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :aggregation [[:aggregation [:sum [:field 1 nil]] {:name "total"}]
                                       [:aggregation [:distinct [:field 2 nil]] {:name "uniq"}]]
                         :limit 5}}
          result (qp/mbql->native query)
          sql (get-in result [:query :sql])]
      (is (str/includes? sql "SUM(\"id\") AS total"))
      (is (str/includes? sql "DISTINCT(\"price\") AS uniq"))
      (is (str/includes? sql "LIMIT 5")))))

(deftest aggregations-cover-common-functions
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :aggregation [[:aggregation [:count] {:name "cnt"}]
                                       [:aggregation [:distinctCount [:field 1 nil]] {:name "dc"}]
                                       [:aggregation [:distinct [:field 2 nil]] {:name "distinct"}]
                                       [:aggregation [:avg [:field 2 nil]] {:name "avg"}]
                                       [:aggregation [:min [:field 2 nil]] {:name "min"}]
                                       [:aggregation [:max [:field 2 nil]] {:name "max"}]
                                       [:aggregation [:percentile [:field 2 nil] [:field 1 nil]] {:name "pct"}]
                                       [:aggregation [:approxMedian [:field 2 nil]] {:name "approx"}]]
                         :limit 1}}
          sql (get-in (qp/mbql->native query) [:query :sql])]
      (doseq [fragment ["COUNT(*) AS cnt"
                        "DISTINCTCOUNT(\"id\") AS dc"
                        "DISTINCT(\"price\") AS distinct"
                        "AVG(\"price\") AS avg"
                        "MIN(\"price\") AS min"
                        "MAX(\"price\") AS max"
                        "PERCENTILE(\"price\", \"id\") AS pct"
                        "APPROXMEDIAN(\"price\") AS approx"]]
        (is (str/includes? sql fragment))))))

(deftest rvalue-handles-temporal-forms
  (let [now (java.time.Instant/parse "2024-01-01T00:00:00Z")]
    (is (string? (#'qp/->rvalue [:absolute-datetime now :day])))
    (is (string? (#'qp/->rvalue [:relative-datetime 1 :day])))
    (is (string? (#'qp/->rvalue [:time now :day])))
    (is (= "field-name" (#'qp/->rvalue [:field "field-name"])))
    (is (= "'example'" (#'qp/->rvalue [:value "example"])))
    (is (= "'raw'" (#'qp/->rvalue "raw")))
    (is (nil? (#'qp/->rvalue nil)))))
