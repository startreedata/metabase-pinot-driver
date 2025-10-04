;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at

;;     http://www.apache.org/licenses/LICENSE-2.0

;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns metabase.driver.pinot-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [metabase.driver :as driver]
   [metabase.driver.pinot]
   [metabase.driver.pinot.execute]))

(deftest pinot-driver-registration-test
  (testing "Pinot driver should be registered"
    (is (some? (get-method driver/database-supports? [:pinot :expression-aggregations])))))

(deftest pinot-driver-features-test
  (testing "Pinot driver should support expected features"
    (is (true? (driver/database-supports? :pinot :expression-aggregations nil)))
    (is (false? (driver/database-supports? :pinot :schemas nil)))
    (is (true? (driver/database-supports? :pinot :set-timezone nil)))
    (is (true? (driver/database-supports? :pinot :temporal/requires-default-unit nil)))))

(deftest pinot-driver-namespace-test
  (testing "Pinot driver namespace should be loadable"
    (is (some? (find-ns 'metabase.driver.pinot)))))

(deftest pinot-driver-connection-test
  (testing "Pinot driver should have connection details"
    (is (some? (driver/connection-properties :pinot)))))

(deftest pinot-driver-native-parameters-test
  (testing "Pinot driver should support native parameters"
    (is (true? (driver/database-supports? :pinot :native-parameters nil)))))

(deftest pinot-driver-template-tag-substitution-test
  (testing "Pinot driver should substitute template tags correctly"
    (let [query {:query "SELECT * FROM table WHERE name = {{name}}"
                 :template-tags {"name" {:name "name" 
                                         :display-name "Name"
                                         :type "text"}}
                 :parameters [{:type "text"
                               :target [:variable [:template-tag "name"]]
                               :value "John"}]}
          result (driver/substitute-native-parameters :pinot query)]
      (is (= "SELECT * FROM table WHERE name = 'John'" (:query result)))
      (is (empty? (:params result))))))

(deftest pinot-driver-optional-template-tag-test
  (testing "Pinot driver should handle optional template tags"
    (let [query {:query "SELECT * FROM table [[WHERE age > {{minAge}}]]"
                 :template-tags {"minAge" {:name "minAge"
                                           :display-name "Minimum Age"
                                           :type "number"}}
                 :parameters [{:type "number"
                               :target [:variable [:template-tag "minAge"]]
                               :value 18}]}
          result (driver/substitute-native-parameters :pinot query)]
      (is (= "SELECT * FROM table WHERE age > 18" (:query result)))
      (is (empty? (:params result))))))

(deftest pinot-driver-missing-optional-template-tag-test
  (testing "Pinot driver should handle missing optional template tags"
    (let [query {:query "SELECT * FROM table [[WHERE age > {{minAge}}]]"
                 :template-tags {"minAge" {:name "minAge"
                                           :display-name "Minimum Age"
                                           :type "number"}}
                 :parameters []}
          result (driver/substitute-native-parameters :pinot query)]
      (is (= "SELECT * FROM table" (:query result)))
      (is (empty? (:params result))))))

(deftest pinot-driver-array-parameter-test
  (testing "Pinot driver should handle array parameters correctly"
    (let [query {:query "SELECT * FROM table WHERE name = {{name}}"
                 :template-tags {"name" {:name "name" 
                                         :display-name "Name"
                                         :type "text"}}
                 :parameters [{:type "text"
                               :target [:variable [:template-tag "name"]]
                               :value ["value"]}]}
          result (driver/substitute-native-parameters :pinot query)]
      (is (= "SELECT * FROM table WHERE name = 'value'" (:query result)))
      (is (empty? (:params result))))))

(deftest pinot-driver-multiple-array-parameter-test
  (testing "Pinot driver should handle multiple array parameters correctly"
    (let [query {:query "SELECT * FROM table WHERE state IN {{states}}"
                 :template-tags {"states" {:name "states" 
                                           :display-name "States"
                                           :type "text"}}
                 :parameters [{:type "text"
                               :target [:variable [:template-tag "states"]]
                               :value ["value 1" "value 2" "value 3"]}]}
          result (driver/substitute-native-parameters :pinot query)]
      (is (= "SELECT * FROM table WHERE state IN ('value 1', 'value 2', 'value 3')" (:query result)))
      (is (empty? (:params result))))))

(deftest pinot-driver-empty-results-test
  (testing "Pinot driver should handle empty result sets without error"
    (let [empty-result {:projections ["DestStateName" "count(*)"]
                        :results []}
          query {:native {:mbql? false}}
          ;; Mock the respond function to capture the result
          captured-result (atom nil)
          respond-fn (fn [metadata rows]
                       (reset! captured-result {:metadata metadata :rows rows}))]
      
      ;; This should not throw an exception
      (is (not (thrown? Exception
                       (metabase.driver.pinot.execute/reduce-results 
                        query empty-result respond-fn))))
      
      ;; Verify the result structure
      (is (some? @captured-result))
      (is (= [] (:rows @captured-result)))
      (is (some? (:metadata @captured-result)))))) 