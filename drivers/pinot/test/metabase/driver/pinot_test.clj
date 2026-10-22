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
(ns metabase.driver.pinot-test
  (:require
   [clj-http.client :as http]
   [clojure.core.async :as async]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [metabase.driver :as driver]
   [metabase.driver.pinot :as pinot]
   [metabase.driver.pinot.query-processor :as pinot.qp]
   [metabase.driver.pinot.sync :as pinot.sync]
   [metabase.driver.util :as driver.util]
   [metabase.driver.sql.parameters.substitution :as sql.params.substitution]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata.protocols :as lib.metadata.protocols]
   [metabase.query-processor.parameters.values :as params.values]
   [metabase.driver.pinot.client :as client]
   [metabase.driver.pinot.execute :as execute]
   [metabase.driver.sql-jdbc.connection.ssh-tunnel :as ssh]))

(deftest driver-registration-test
  (testing "Pinot driver is registered and exposes expected capabilities"
    (is (contains? (driver.util/available-drivers) :pinot))
    (is (true? (driver/database-supports? :pinot :expression-aggregations nil)))
    (is (false? (driver/database-supports? :pinot :schemas nil)))
    (is (= :sunday (driver/db-start-of-week :pinot)))))

(deftest can-connect?-test
  (let [details {:controller-endpoint "http://pinot:9000"
                 :auth-enabled true
                 :auth-token-type "Bearer"
                 :auth-token-value "token"
                 :database-name "analytics"}]
    (testing "successful connection returns true and uses auth headers"
      (with-redefs [ssh/do-with-ssh-tunnel (fn [config f] (f config))
                    http/get (fn [url options]
                               (is (= "http://pinot:9000/health" url))
                               (is (= {"Authorization" "Bearer token"
                                       "database" "analytics"}
                                      (:headers options)))
                               {:status 200})]
        (is (true? (driver/can-connect? :pinot details)))))

    (testing "connection without auth uses empty headers"
      (with-redefs [ssh/do-with-ssh-tunnel (fn [config f] (f config))
                    http/get (fn [_ options]
                               (is (empty? (:headers options)))
                               {:status 200})]
        (is (true? (driver/can-connect? :pinot {:controller-endpoint "http://pinot:9000"
                                                :auth-enabled false})))))

    (testing "non-200 status is treated as a failed connection"
      (with-redefs [ssh/do-with-ssh-tunnel (fn [config f] (f config))
                    http/get (fn [_ _] {:status 500})]
        (is (false? (driver/can-connect? :pinot {:controller-endpoint "http://pinot:9000"})))))))

(deftest execute-reducible-query-adds-timeout
  (testing "execute-reducible-query wraps SQL strings with a timeout map"
    (let [captured (atom nil)
          context {:canceled-chan (async/chan)}
          respond (fn [_ _])]
      (with-redefs [execute/execute-reducible-query
                    (fn [runner query respond-fn]
                      (reset! captured {:runner runner :query query})
                      (respond-fn {:cols []} []))
                    client/do-query-with-cancellation
                    (fn [_ details query]
                      {:details details :query query})]
        (driver/execute-reducible-query
         :pinot
         {:qp/compiled {:query "SELECT 1"}}
         context
         respond))

      (is (= 30000 (get-in @captured [:query :qp/compiled :query :queryOptions :timeoutMs])))
      (is (= {:sql "SELECT 1" :queryOptions {:timeoutMs 30000}}
             (get-in @captured [:query :qp/compiled :query]))))))

(def ^:private parameter-metadata
  (reify lib.metadata.protocols/MetadataProvider
    (database [_]
      {:lib/type :metadata/database :id 1 :engine :pinot :name "Pinot" :features #{:native-parameters}})
    (metadatas [_ spec]
      (into []
            (comp (filter #(= (:lib/type %) (:lib/type spec)))
                  (lib.metadata.protocols/default-spec-filter-xform spec))
            [{:lib/type :metadata/table :id 1 :db-id 1 :name "orders" :display-name "Orders"}
             {:lib/type :metadata/column :id 2 :table-id 1 :name "state" :display-name "State"
              :base-type :type/Text :effective-type :type/Text}]))
    (setting [_ _] nil)))

(defn- parameter-query
  [sql tags]
  {:lib/type :mbql/query
   :lib/metadata parameter-metadata
   :database 1
   :stages [{:lib/type :mbql.stage/native
             :native sql
             :template-tags tags}]})

(deftest substitute-native-parameters-inlines-values
  (binding [driver/*driver* :pinot]
    (doseq [[label sql tags expected]
            [["number" "SELECT {{value}}" [{:name "value" :display-name "Value" :type :number :default 100}]
              "SELECT 100"]
             ["quoted string and literal question mark" "SELECT '?', {{value}}"
              [{:name "value" :display-name "Value" :type :text :default "O'Reilly?"}]
              "SELECT '?', 'O''Reilly?'"]
             ["date" "SELECT {{value}}" [{:name "value" :display-name "Value" :type :date :default "2024-01-01"}]
              "SELECT '2024-01-01'"]
             ["absent optional" "SELECT 1 [[WHERE state = {{value}}]]"
              [{:name "value" :display-name "Value" :type :text}] "SELECT 1"]
             ["present optional" "SELECT 1 [[WHERE state = {{value}}]]"
              [{:name "value" :display-name "Value" :type :text :default "CA"}] "SELECT 1 WHERE state = 'CA'"]
             ["field filter" "SELECT * FROM orders WHERE {{state}}"
              [{:name "state" :display-name "State" :type :dimension
                :dimension [:field {:lib/uuid "1d31d178-252e-40f1-ab81-4b61a9c932fa"} 2]
                :widget-type :category :default "CA"}]
              "SELECT * FROM orders WHERE \"orders\".\"state\" = 'CA'"]]]
      (testing label
        (let [result (driver/substitute-native-parameters-in-stage :pinot (parameter-query sql tags) 0)
              stage (lib/query-stage result 0)]
          (is (= expected (:native stage)))
          (is (empty? (:params stage))))))))

(deftest substitute-native-parameters-tracks-card-ids
  (testing "referenced card ids are merged into permission metadata"
    (let [query (assoc-in (parameter-query "SELECT {{card}}" [])
                          [:stages 0 :query-permissions/referenced-card-ids] #{1})]
      (with-redefs [params.values/stage->params-map
                    (constantly {"card" (lib/parsed-referenced-card-query-param 2 "SELECT '?', ?" [100])})]
        (binding [driver/*driver* :pinot]
          (let [stage (lib/query-stage (driver/substitute-native-parameters-in-stage :pinot query 0) 0)]
            (is (= "SELECT (SELECT '?', 100)" (:native stage)))
            (is (= #{1 2} (:query-permissions/referenced-card-ids stage)))))))))

(deftest add-timeout-to-query-test
  (testing "SQL strings are wrapped in Pinot query maps with a timeout"
    (is (= {:sql "SELECT 1" :queryOptions {:timeoutMs 5000}}
           (#'pinot/add-timeout-to-query "SELECT 1" 5000))))

  (testing "Existing query maps keep existing options when adding a timeout"
    (is (= {:sql "SELECT 1" :queryOptions {:foo true :timeoutMs 5000}}
           (#'pinot/add-timeout-to-query {:sql "SELECT 1"
                                          :queryOptions {:foo true}}
                                         5000))))

  (testing "JSON query strings are parsed before adding a timeout"
    (is (= {:sql "SELECT 1" :queryOptions {:timeoutMs 5000}}
           (#'pinot/add-timeout-to-query "{\"sql\":\"SELECT 1\"}" 5000))))

  (testing "Invalid JSON string falls back to SQL wrapper with timeout"
    (let [result (#'pinot/add-timeout-to-query "not json at all" 5000)]
      (is (= "not json at all" (:sql result)))
      (is (= 5000 (get-in result [:queryOptions :timeoutMs])))))

  (testing "JSON string that fails to parse falls back to SQL wrapper"
    (let [result (#'pinot/add-timeout-to-query "{invalid" 5000)]
      (is (= "{invalid" (:sql result)))
      (is (= 5000 (get-in result [:queryOptions :timeoutMs]))))))

(deftest native-parameters-capability-test
  (testing "Pinot driver declares native SQL parameter support for Metabase native editor"
    (is (true? (driver/database-supports? :pinot :native-parameters nil)))))

(deftest pinot-literal-and-inline-params-test
  (testing "pinot-literal safely formats values"
    (is (= "NULL" (#'pinot/pinot-literal nil)))
    (is (= "'O''Reilly'" (#'pinot/pinot-literal "O'Reilly")))
    (is (= "TRUE" (#'pinot/pinot-literal true)))
    (is (= "FALSE" (#'pinot/pinot-literal false)))
    (is (= "5" (#'pinot/pinot-literal 5)))
    (is (= "'123e4567-e89b-12d3-a456-426614174000'"
           (#'pinot/pinot-literal (java.util.UUID/fromString "123e4567-e89b-12d3-a456-426614174000"))))
    (is (re-find #"1970"
                 (#'pinot/pinot-literal (java.util.Date. 0))))
    (is (str/includes? (#'pinot/pinot-literal (java.time.LocalDate/of 2024 1 1)) "2024"))
    (is (str/includes? (#'pinot/pinot-literal (java.time.Instant/parse "2024-01-01T00:00:00Z")) "2024")))

  (testing "truncate-for-logging shortens long messages"
    (is (= "abc... (truncated)" (#'pinot/truncate-for-logging "abcdefghijklmnopqrstuvwxyz" 3)))
    (is (= "short" (#'pinot/truncate-for-logging "short" 10))))

  (testing "inline-params replaces positional placeholders"
    (is (= "select * from t where id = 10 and name = 'foo'"
           (#'pinot/inline-params "select * from t where id = ? and name = ?" [10 "foo"])))
    (is (= "select * from t where deleted = NULL"
           (#'pinot/inline-params "select * from t where deleted = ?" [nil])))
    (is (= "select 1"
           (#'pinot/inline-params "select 1" [42])))
    (is (= "select ?"
           (#'pinot/inline-params "select ?" nil)))
    (is (= "SELECT '?', \"?\", 'value?', 2 -- ?\n/* ? */"
           (#'pinot/inline-params "SELECT '?', \"?\", ?, ? -- ?\n/* ? */" ["value?" 2])))))

(deftest replacement-snippet-info-inlines-sql-impl
  (is (= {:replacement-snippet "NULL" :prepared-statement-args []}
         (sql.params.substitution/->replacement-snippet-info :pinot nil)))
  (is (= {:replacement-snippet "'abc'" :prepared-statement-args []}
         (sql.params.substitution/->replacement-snippet-info :pinot "abc"))))

(deftest driver-defmethods-delegate-to-implementations
  (testing "driver multimethods forward to sync/query processor implementations"
    (let [calls (atom [])]
      (with-redefs [pinot.sync/describe-table (fn [& args]
                                                (swap! calls conj [:describe-table args])
                                                :table)
                    pinot.sync/describe-database (fn [& args]
                                                   (swap! calls conj [:describe-database args])
                                                   :database)
                    pinot.sync/dbms-version (fn [& args]
                                              (swap! calls conj [:dbms-version args])
                                              :version)
                    pinot.qp/mbql->native (fn [& args]
                                            (swap! calls conj [:mbql->native args])
                                            :native)]
        (is (= :table (driver/describe-table :pinot {:details {}} {:name "users"})))
        (is (= :database (driver/describe-database :pinot {:details {}})))
        (is (= :version (driver/dbms-version :pinot {:details {}})))
        (is (= :native (driver/mbql->native :pinot {:query {:source-table 1}})))
        (is (= #{:describe-table :describe-database :dbms-version :mbql->native}
               (set (map first @calls))))))))

(deftest pinot-driver-empty-results-test
  (testing "Pinot driver should handle empty result sets without error"
    (let [empty-result {:projections ["DestStateName" "count(*)"]
                        :results []}
          query {:qp/compiled {:mbql? false}}
          ;; Mock the respond function to capture the result
          captured-result (atom nil)
          respond-fn (fn [metadata rows]
                       (reset! captured-result {:metadata metadata :rows rows}))]
      
      ;; This should not throw an exception (if it does, the test fails)
      (metabase.driver.pinot.execute/reduce-results
       query empty-result respond-fn)
      
      ;; Verify the result structure
      (is (some? @captured-result))
      (is (= [] (:rows @captured-result)))
      (is (some? (:metadata @captured-result))))))
