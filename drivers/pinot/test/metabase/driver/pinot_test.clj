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
   [metabase.driver.pinot]))

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