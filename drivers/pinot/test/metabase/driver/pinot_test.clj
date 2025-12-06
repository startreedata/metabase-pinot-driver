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
   [metabase.driver.common.parameters.parse :as params.parse]
   [metabase.driver.common.parameters.values :as params.values]
   [metabase.driver.sql.parameters.substitute :as sql.params.substitute]
   [metabase.driver.sql.parameters.substitution :as sql.params.substitution]
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
         {:native {:query "SELECT 1"}}
         context
         respond))

      (is (= 30000 (get-in @captured [:query :native :query :queryOptions :timeoutMs])))
      (is (= {:sql "SELECT 1" :queryOptions {:timeoutMs 30000}}
             (get-in @captured [:query :native :query]))))))

(deftest substitute-native-parameters-inlines-values
  (testing "native parameter substitution inlines values into SQL"
    (let [query {:query "select * from orders where id = ?"}]
      (with-redefs [params.values/query->params-map (constantly {})
                    params.values/referenced-card-ids (constantly #{})
                    params.parse/parse (fn [sql] [sql []])
                    sql.params.substitute/substitute (fn [_ _] ["select * from orders where id = ?" [100]])]
         (let [result (driver/substitute-native-parameters :pinot query)]
           (is (= "select * from orders where id = 100" (:query result)))
          (is (= [100] (:params result))))))))

(deftest substitute-native-parameters-tracks-card-ids
  (testing "referenced card ids are merged into permission metadata"
    (let [query {:query "select 1"
                 :query-permissions/referenced-card-ids #{1}}]
      (with-redefs [params.values/query->params-map (constantly {})
                    params.values/referenced-card-ids (constantly #{2})
                    params.parse/parse (fn [sql] [sql []])
                    sql.params.substitute/substitute (fn [[sql params] _] [sql params])]
        (let [result (driver/substitute-native-parameters :pinot query)]
          (is (= #{1 2} (:query-permissions/referenced-card-ids result))))))))

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
           (#'pinot/add-timeout-to-query "{\"sql\":\"SELECT 1\"}" 5000)))))

(deftest pinot-literal-and-inline-params-test
  (testing "pinot-literal safely formats values"
    (is (= "NULL" (#'pinot/pinot-literal nil)))
    (is (= "'O''Reilly'" (#'pinot/pinot-literal "O'Reilly")))
    (is (= "TRUE" (#'pinot/pinot-literal true)))
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
           (#'pinot/inline-params "select ?" nil)))))

(deftest replacement-snippet-info-uses-sql-impl
  (is (= (sql.params.substitution/->replacement-snippet-info :sql nil)
         (sql.params.substitution/->replacement-snippet-info :pinot nil)))
  (is (= (sql.params.substitution/->replacement-snippet-info :sql "abc")
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
