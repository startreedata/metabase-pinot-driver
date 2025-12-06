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
(ns metabase.driver.pinot.sync-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [metabase.driver.pinot.client :as client]
   [metabase.driver.pinot.sync :as sync]
   [metabase.driver.sql-jdbc.connection.ssh-tunnel :as ssh]
   [metabase.secrets.models.secret :as secret]))

(def base-details
  {:controller-endpoint "http://localhost:9000"
   :auth-enabled true
   :auth-token-type "Bearer"
   :auth-token-value "secret"})

(deftest describe-table-builds-field-metadata
  (with-redefs [ssh/do-with-ssh-tunnel (fn [details f] (f details))
                secret/value-as-string (fn [_ details _] (:auth-token-value details))
                client/GET (fn [url & _]
                             (is (= "http://localhost:9000/tables/users/schema" url))
                             {:dimensionFieldSpecs [{:name "id" :dataType "LONG"}]
                              :metricFieldSpecs [{:name "amount" :dataType "DOUBLE"}]
                              :dateTimeFieldSpecs [{:name "created_at"
                                                    :dataType "TIMESTAMP"
                                                    :format "1:MILLISECONDS:EPOCH"
                                                    :granularity "DAY"}]})]
    (let [result (sync/describe-table {:details base-details} {:name "users"})
          fields-by-name (into {} (map (juxt :name identity) (:fields result)))]
      (is (= "users" (:name result)))
      (is (= #{:type/Integer :type/Float :type/Time}
             (set (map :base-type (vals fields-by-name)))))
      (is (= "DAY" (:granularity (fields-by-name "created_at")))))))

(deftest describe-database-strips-schema
  (with-redefs [ssh/do-with-ssh-tunnel (fn [details f] (f details))
                secret/value-as-string (fn [_ details _] (:auth-token-value details))
                client/GET (fn [url & _]
                             (is (= "http://localhost:9000/tables" url))
                             {:tables ["foo" "schema.bar"]})]
    (let [result (sync/describe-database {:details base-details})
          names (set (map :name (:tables result)))]
      (is (= #{"foo" "bar"} names)))))

(deftest dbms-version-handles-health-and-version-endpoints
  (testing "when a database is provided use /health and default to latest"
    (with-redefs [ssh/do-with-ssh-tunnel (fn [details f] (f (assoc details :database-name "analytics")))
                  secret/value-as-string (fn [_ details _] (:auth-token-value details))
                  client/GET (fn [url & {:keys [as]}]
                               (is (= "http://localhost:9000/health" url))
                               (is (= :text as))
                               "OK")]
      (is (= {:version "latest"}
             (sync/dbms-version {:details base-details})))))

  (testing "without database-name use /version"
    (with-redefs [ssh/do-with-ssh-tunnel (fn [details f] (f (dissoc details :database-name)))
                  secret/value-as-string (fn [_ details _] (:auth-token-value details))
                  client/GET (fn [url & _]
                               (is (= "http://localhost:9000/version" url))
                               {:pinot-segment-uploader-default "1.2.0"})]
      (is (= {:version "1.2.0"}
             (sync/dbms-version {:details base-details})))))) 
