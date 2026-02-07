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
   [clojure.test :refer [deftest is testing]]
   [metabase.driver.pinot.query-processor :as qp]
   [metabase.driver.sql.query-processor :as sql.qp]
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

(deftest filter-with-lower-for-search-box
  (testing "Search box / case-insensitive filter uses LOWER() so Pinot can match with %value% semantics."
    (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :filter [:= [:lower [:field 2 nil]] "searchterm"]}}
          result (qp/mbql->native query)
          sql (get-in result [:query :sql])]
      (is (str/includes? sql "LOWER(\"price\")"))
      (is (str/includes? sql "searchterm"))))))

(deftest rvalue-handles-temporal-forms
  (let [now (java.time.Instant/parse "2024-01-01T00:00:00Z")]
    (is (string? (#'qp/->rvalue [:absolute-datetime now :day])))
    (is (string? (#'qp/->rvalue [:absolute-datetime now :default])))
    (is (string? (#'qp/->rvalue [:relative-datetime 1 :day])))
    (is (string? (#'qp/->rvalue [:time now :day])))
    (is (= "field-name" (#'qp/->rvalue [:field "field-name"])))
    (is (= "'example'" (#'qp/->rvalue [:value "example"])))
    (is (= "'raw'" (#'qp/->rvalue "raw")))
    (is (= "NULL" (#'qp/->rvalue nil)))
    (is (= 42 (#'qp/->rvalue 42)))
    (is (= 3.14 (#'qp/->rvalue 3.14)))))

(deftest inline-value-handles-pinot-string
  (is (= "'hello'" (sql.qp/inline-value :pinot "hello"))))

(deftest rvalue-field-integer-resolves-metadata-name
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (is (= "id" (#'qp/->rvalue [:field 1])))))

(deftest resolve-field-string-id-and-options
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (is (= "\"dropdown_column\"" (#'qp/resolve-field [:field "dropdown_column" nil])))
    ;; options map is used when field-id is not integer/string (e.g. keyword)
    (is (= "\"from_options\"" (#'qp/resolve-field [:field :id {:name "from_options"}])))))

(deftest resolve-field-else-unknown-type
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (is (= "\"unknown-field-:custom\"" (#'qp/resolve-field [:field :custom nil])))))

(deftest resolve-field-invalid-structure-throws
  ;; [:field] has < 2 elements so we throw without needing the store
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"Invalid field clause structure"
                        (#'qp/resolve-field [:field]))))

(deftest filter-contains-search-box
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :filter [:contains [:field 2 nil] [:value "partial"]]}}
          result (qp/mbql->native query)
          sql (get-in result [:query :sql])]
      (is (str/includes? sql "LIKE"))
      (is (str/includes? sql "%partial%")))))

(deftest filter-not-equal-null-becomes-is-not-null
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :filter [:!= [:field 1 nil] nil]}}
          result (qp/mbql->native query)
          where (get-in result [:query :where])]
      (is (str/includes? (str where) "IS NOT NULL")))))

(deftest query-with-no-filter
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil] [:field 2 nil]]
                         :limit 5}}
          result (qp/mbql->native query)]
      (is (nil? (get-in result [:query :where])))
      (is (= "SELECT \"id\", \"price\" FROM events LIMIT 5"
             (get-in result [:query :sql]))))))

(deftest query-select-only-aggregations-no-breakout
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :aggregation [[:aggregation [:count] {:name "cnt"}]]
                         :limit 1}}
          result (qp/mbql->native query)
          sql (get-in result [:query :sql])]
      (is (str/includes? sql "COUNT(*) AS cnt"))
      (is (str/includes? sql "FROM events"))
      (is (str/includes? sql "LIMIT 1")))))

(deftest query-with-no-limit
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]}}
          result (qp/mbql->native query)]
      (is (nil? (get-in result [:query :limit])))
      (is (= "SELECT \"id\" FROM events" (get-in result [:query :sql]))))))

(deftest query-with-page-offset
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :limit 10
                         :page 3}}
          result (qp/mbql->native query)
          sql (get-in result [:query :sql])]
      (is (= 30 (get-in result [:query :offset])))
      (is (str/includes? sql "LIMIT 30, 10")))))

(deftest source-table-from-source-query-native
  (with-redefs [qp.store/metadata-provider (constantly {})
                lib.metadata/table (fn [_ _] nil)
                lib.metadata/field (fn [_ _] nil)]
    (let [query {:database 1
                 :type :query
                 :query {:source-query {:native "SELECT * FROM my_custom_table"}
                         :fields [[:field "x" nil]]}}
          result (qp/mbql->native query)]
      (is (= "my_custom_table" (get-in result [:query :dataSource]))))))

(deftest source-query-native-with-no-from-leaves-datasource-unset
  (with-redefs [qp.store/metadata-provider (constantly {})
                lib.metadata/table (fn [_ _] nil)
                lib.metadata/field (fn [_ _] nil)]
    (let [query {:database 1
                 :type :query
                 :query {:source-query {:native "SELECT 1"}
                         :fields [[:field "x" nil]]}}
          result (qp/mbql->native query)]
      (is (nil? (get-in result [:query :dataSource])))
      (is (str/includes? (get-in result [:query :sql]) "FROM ")))))

(deftest mbql-to-native-throws-on-invalid-query
  (with-redefs [qp.store/metadata-provider (fn [] (throw (ex-info "no provider" {})))
                lib.metadata/table (fn [_ _] (throw (RuntimeException. "bad")))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Error generating Pinot"
                          (qp/mbql->native {:query {:source-table 1}
                                            :settings {}})))))

(deftest filter-unsupported-op-throws
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    ;; mbql->native wraps any throw in "Error generating Pinot query"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Error generating Pinot"
                          (qp/mbql->native {:database 1
                                            :type :query
                                            :query {:source-table 1
                                                    :fields [[:field 1 nil]]
                                                    :filter [:does-not-exist [:field 1 nil] 1]}})))))

(deftest filter-invalid-format-throws
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    ;; mbql->native wraps any throw in "Error generating Pinot query"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Error generating Pinot"
                          (qp/mbql->native {:database 1
                                            :type :query
                                            :query {:source-table 1
                                                    :fields [[:field 1 nil]]
                                                    :filter "not a vector"}})))))

(deftest query-with-empty-fields
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields []
                         :limit 5}}
          result (qp/mbql->native query)]
      (is (= [] (get-in result [:query :columns])))
      (is (str/includes? (get-in result [:query :sql]) "FROM events")))))

(deftest query-with-order-by-desc
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :order-by [[:desc [:field 2 nil]]]
                         :limit 5}}
          result (qp/mbql->native query)
          sql (get-in result [:query :sql])]
      (is (= ["\"price\" desc"] (get-in result [:query :order-by])))
      (is (str/includes? sql "desc")))))

(deftest query-aggregation-without-name
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :aggregation [[:aggregation [:sum [:field 2 nil]]]]
                         :limit 1}}
          result (qp/mbql->native query)
          sql (get-in result [:query :sql])]
      (is (str/includes? sql "SUM(\"price\")"))
      (is (not (str/includes? sql " AS "))))))

(deftest query-with-no-aggregations
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil] [:field 2 nil]]
                         :limit 3}}
          result (qp/mbql->native query)]
      (is (nil? (get-in result [:query :aggregations])))
      (is (= "SELECT \"id\", \"price\" FROM events LIMIT 3"
             (get-in result [:query :sql]))))))

(deftest query-multiple-order-by
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil] [:field 2 nil]]
                         :order-by [[:asc [:field 1 nil]] [:desc [:field 2 nil]]]
                         :limit 5}}
          result (qp/mbql->native query)
          sql (get-in result [:query :sql])]
      (is (= ["\"id\" asc" "\"price\" desc"] (get-in result [:query :order-by])))
      (is (str/includes? sql "asc"))
      (is (str/includes? sql "desc")))))

(deftest filter-with-value-clause-numeric
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :filter [:= [:field 2 nil] [:value 99]]}}
          result (qp/mbql->native query)
          sql (get-in result [:query :sql])]
      (is (str/includes? sql "99"))
      (is (str/includes? sql "\"price\"")))))

(deftest resolve-field-lower-with-string-field
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (is (= "LOWER(\"search_col\")"
           (#'qp/resolve-field [:lower [:field "search_col" nil]])))))

(deftest handle-breakout-empty-unchanged
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :limit 5}}
          result (qp/mbql->native query)]
      (is (nil? (get-in result [:query :group-by]))))))

(deftest handle-order-by-empty-unchanged
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :limit 5}}
          result (qp/mbql->native query)]
      (is (nil? (get-in result [:query :order-by]))))))

(deftest filter-with-between-three-args
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :filter [:between [:field 2 nil] 10 20]}}
          result (qp/mbql->native query)
          sql (get-in result [:query :sql])]
      (is (str/includes? sql "BETWEEN"))
      (is (str/includes? sql "10"))
      (is (str/includes? sql "20")))))

(deftest filter-and-nested
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [query {:database 1
                 :type :query
                 :query {:source-table 1
                         :fields [[:field 1 nil]]
                         :filter [:and [:= [:field 1 nil] 1] [:and [:= [:field 2 nil] 2] [:= [:field 1 nil] 3]]]}}
          result (qp/mbql->native query)
          where (get-in result [:query :where])]
      (is (str/includes? (str where) "AND"))
      (is (str/includes? (str where) "price")))))

(deftest filter-less-than
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [r (qp/mbql->native {:database 1 :type :query
                              :query {:source-table 1 :fields [[:field 1 nil]]
                                      :filter [:< [:field 2 nil] 25]}})]
      (is (str/includes? (get-in r [:query :sql]) "<")))))

(deftest filter-less-than-or-equal
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [r (qp/mbql->native {:database 1 :type :query
                              :query {:source-table 1 :fields [[:field 1 nil]]
                                      :filter [:<= [:field 2 nil] 25]}})]
      (is (str/includes? (get-in r [:query :sql]) "<=")))))

(deftest filter-greater-than
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [r (qp/mbql->native {:database 1 :type :query
                              :query {:source-table 1 :fields [[:field 1 nil]]
                                      :filter [:> [:field 2 nil] 10]}})]
      (is (str/includes? (get-in r [:query :sql]) ">")))))

(deftest filter-greater-than-or-equal
  (with-redefs [qp.store/metadata-provider (:qp.store/metadata-provider redefine-metadata!)
                lib.metadata/table (:lib.metadata/table redefine-metadata!)
                lib.metadata/field (:lib.metadata/field redefine-metadata!)]
    (let [r (qp/mbql->native {:database 1 :type :query
                              :query {:source-table 1 :fields [[:field 1 nil]]
                                      :filter [:>= [:field 2 nil] 10]}})]
      (is (str/includes? (get-in r [:query :sql]) ">=")))))
