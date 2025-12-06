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
(ns metabase.driver.pinot.execute-test
  (:require
   [clojure.test :refer [deftest is]]
   [metabase.driver.pinot.execute :as execute]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.query-processor.middleware.annotate :as annotate]
   [metabase.query-processor.store :as qp.store]))

(deftest post-process-turns-rows-into-maps
  (let [results {:resultTable {:dataSchema {:columnNames ["count" "name"]}
                               :rows [[1 "foo"] [2 "bar"]]}}
        processed (#'execute/post-process results)]
    (is (= ["count" "name"] (:projections processed)))
    (is (= [{"count" 1 "name" "foo"}
            {"count" 2 "name" "bar"}]
           (:results processed)))))

(deftest post-process-handles-missing-data
  (is (= {:projections nil :results []}
         (#'execute/post-process {:resultTable {:dataSchema {:columnNames nil}
                                                :rows nil}}))))

(defn fake-base-type-inferer [_metadata]
  (fn
    ([] [])
    ([acc] acc)
    ([acc row]
     (if (seq acc)
       acc
       (vec (repeat (count row) :type/Integer))))))

(deftest reduce-results-builds-metadata-and-rows
  (let [result {:projections ["count"]
                :results [{"count" 5} {"count" 6}]}
        captured (atom nil)]
  (with-redefs [annotate/merged-column-info (fn [_ metadata] (:cols metadata))
                annotate/base-type-inferer fake-base-type-inferer]
    (#'execute/reduce-results
     {:native {:mbql? true}}
     result
       (fn [metadata rows]
         (reset! captured {:metadata metadata :rows rows})))
      (is (= [{:name "count" :base_type :type/Integer}]
             (get-in @captured [:metadata :cols])))
      (is (= [[5] [6]] (:rows @captured))))))

(deftest remove-bonus-keys-strips-temp-columns
  (is (= [:a :b]
         (#'execute/remove-bonus-keys [:a :___temp :b :___meta]))))

(deftest result-rows-errors-when-column-missing
  (is (thrown? Exception
               (#'execute/result-rows {:results [{"count" 1}]}
                                      ["count" "missing"]
                                      [:count :missing]))))

(deftest result-metadata-normalizes-count-columns
  (is (= [{:name "count" :base_type :type/*}]
         (:cols (#'execute/result-metadata [:distinct___count])))))

(deftest execute-reducible-query-runs-end-to-end
  (let [captured (atom nil)]
    (with-redefs [qp.store/metadata-provider (constantly {:details {}})
                  lib.metadata/database (fn [provider] provider)
                  annotate/merged-column-info (fn [_ metadata] (:cols metadata))
                  annotate/base-type-inferer fake-base-type-inferer]
      (execute/execute-reducible-query
       (fn [_details query]
         ;; ensure JSON queries are parsed before execution
         (is (= {:sql "SELECT 1"} query))
         {:resultTable {:dataSchema {:columnNames ["count"]} :rows [[3]]}})
       {:native {:query "{\"sql\":\"SELECT 1\"}" :mbql? true}}
       (fn [metadata rows]
         (reset! captured {:metadata metadata :rows rows}))))
    (is (= [[3]] (:rows @captured)))
    (is (= [{:name "count" :base_type :type/Integer}]
           (get-in @captured [:metadata :cols])))))

(deftest execute-reducible-query-wraps-exceptions
  (with-redefs [qp.store/metadata-provider (constantly {:details {}})
                lib.metadata/database (fn [provider] provider)]
    (is (thrown? Exception
                 (execute/execute-reducible-query
                  (fn [_ _] (throw (RuntimeException. "boom")))
                  {:native {:query {:sql "SELECT 1"}}}
                  (fn [_ _]))))))

(deftest reduce-results-handles-native-queries
  (let [captured (atom nil)]
    (with-redefs [annotate/merged-column-info (fn [_ metadata] (:cols metadata))
                  annotate/base-type-inferer fake-base-type-inferer]
      (#'execute/reduce-results
       {:native {:mbql? false}}
       {:results [{"col" 1} {"col" 2}]}
       (fn [metadata rows]
         (reset! captured {:metadata metadata :rows rows}))))
    (is (= [[1] [2]] (:rows @captured)))
    (is (= [{:name "col" :base_type :type/Integer}]
           (get-in @captured [:metadata :cols])))))
