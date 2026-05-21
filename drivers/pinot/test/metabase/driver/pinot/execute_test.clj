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
   [clojure.test :refer [deftest is testing]]
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

(deftest post-process-wraps-mapping-errors
  (with-redefs [clojure.core/zipmap (fn [& _] (throw (RuntimeException. "zip-fail")))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Error processing Pinot query results"
                          (#'execute/post-process {:resultTable {:dataSchema {:columnNames ["c"]}
                                                                 :rows [[1]]}})))))

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

(deftest result-rows-require-column-names
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"No valid column names provided"
                        (#'execute/result-rows {:results []} nil nil))))

(deftest result-metadata-normalizes-count-columns
  (is (= [{:name "count" :base_type :type/*}]
         (:cols (#'execute/result-metadata [:distinct___count])))))

(deftest result-metadata-normalizes-timestamp-column
  (is (= [{:name "timestamp" :base_type :type/*}]
         (:cols (#'execute/result-metadata [:timestamp___int])))))

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

(deftest execute-reducible-query-wraps-post-process-errors
  (with-redefs [qp.store/metadata-provider (constantly {:details {}})
                lib.metadata/database (fn [provider] provider)
                execute/post-process (fn [_] (throw (RuntimeException. "post-fail")))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Error post-processing Pinot query results"
                          (execute/execute-reducible-query
                           (fn [_ _] {:resultTable {}})
                           {:native {:query {:sql "SELECT 1"}}}
                           (fn [_ _]))))))

(deftest execute-reducible-query-wraps-reduce-errors
  (with-redefs [qp.store/metadata-provider (constantly {:details {}})
                lib.metadata/database (fn [provider] provider)
                execute/post-process identity
                execute/reduce-results (fn [& _] (throw (RuntimeException. "reduce-fail")))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Error reducing Pinot query results"
                          (execute/execute-reducible-query
                           (fn [_ _] {:native {:query {}}})
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

(deftest execute-reducible-query-throws-when-pinot-returns-exceptions
  (testing "When Pinot response contains :exceptions (e.g. SQL parse error), driver throws so Metabase UI can display the error."
    (with-redefs [qp.store/metadata-provider (constantly {:details {}})
                lib.metadata/database (fn [provider] provider)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Pinot query error"
                          (execute/execute-reducible-query
                           (fn [_ _] {:exceptions [{:message "Unknown column 'foo'"}
                                                    {:message "Syntax error at line 1"}]})
                           {:native {:query {:sql "SELECT foo"}}}
                           (fn [_ _])))))
  (with-redefs [qp.store/metadata-provider (constantly {:details {}})
                lib.metadata/database (fn [provider] provider)]
    (try
      (execute/execute-reducible-query
       (fn [_ _] {:exceptions [{:message "Unknown column 'bar'"}]})
       {:native {:query {:sql "SELECT bar"}}}
       (fn [_ _]))
      (is false "expected exception")
      (catch clojure.lang.ExceptionInfo e
        (is (= :invalid-query (get-in (ex-data e) [:type]))
            "Pinot errors should have :type :invalid-query (a :client error) so Metabase classifies them as 4xx user errors rather than 5xx server errors"))))))

(deftest pinot-exceptions-propagate-with-client-error-metadata
  (testing "When Pinot returns errors in the :exceptions array, the driver throws an ex-info
            whose ex-data tells Metabase to render the message inline in the UI as a 4xx
            client error (rather than a generic 5xx 'We're experiencing server issues' banner)."
    (with-redefs [qp.store/metadata-provider (constantly {:details {}})
                  lib.metadata/database (fn [provider] provider)]
      (let [pinot-exception {:errorCode 150 :message "Some Pinot error"}]
        (try
          (execute/execute-reducible-query
           (fn [_ _] {:exceptions [pinot-exception]})
           {:native {:query {:sql "SELECT foo"}}}
           (fn [_ _]))
          (is false "expected exception")
          (catch clojure.lang.ExceptionInfo e
            (let [data (ex-data e)]
              (is (= :invalid-query (:type data))
                  ":client error in the qp.error-type hierarchy, not :db (a :server error)")
              (is (= 400 (:status-code data))
                  "explicit status-code so streaming-response/exception middleware returns 4xx, not 5xx")
              (is (true? (:is-curated data))
                  "signals to the FE that Pinot's error message is safe to render verbatim")
              (is (re-find #"Some Pinot error" (ex-message e))
                  "original Pinot message must reach the user, not be replaced by a generic wrapper")
              (is (= [pinot-exception]
                     (:exceptions data))
                  "raw Pinot exception payload preserved in ex-data for introspection"))))))))

(deftest reduce-results-empty-native-uses-projections
  (testing "When native query has no rows, column names come from schema projections (empty result handling)."
    (let [captured (atom nil)]
    (with-redefs [annotate/merged-column-info (fn [_ metadata] (:cols metadata))
                  annotate/base-type-inferer fake-base-type-inferer]
      (#'execute/reduce-results
       {:type :query :native {:mbql? false}}
       {:projections ["a" "b"] :results []}
       (fn [metadata rows]
         (reset! captured {:metadata metadata :rows rows}))))
    (is (= [] (:rows @captured)))
    (is (some? (:metadata @captured))))))

(deftest post-process-rows-nil-returns-empty
  (is (= {:projections nil :results []}
         (#'execute/post-process {:resultTable {:dataSchema {:columnNames ["a" "b"]}
                                                 :rows nil}}))))

(deftest reduce-results-mbql-uses-remove-bonus-keys
  (let [captured (atom nil)]
    (with-redefs [annotate/merged-column-info (fn [_ metadata] (:cols metadata))
                  annotate/base-type-inferer fake-base-type-inferer]
      (#'execute/reduce-results
       {:native {:mbql? true}}
       {:projections ["id" "___temp" "name"]
        :results [{"id" 1 "___temp" 0 "name" "x"}]}
       (fn [metadata rows]
         (reset! captured {:metadata metadata :rows rows}))))
    (is (= ["id" "name"] (mapv :name (get-in @captured [:metadata :cols]))))))

(deftest reduce-results-native-no-projections-empty-results
  (let [captured (atom nil)]
    (with-redefs [annotate/merged-column-info (fn [_ metadata] (:cols metadata))
                  annotate/base-type-inferer fake-base-type-inferer]
      (#'execute/reduce-results
       {:native {:mbql? false}}
       {:projections []
        :results []}
       (fn [metadata rows]
         (reset! captured {:metadata metadata :rows rows}))))
    (is (= [] (get-in @captured [:metadata :cols])))
    (is (= [] (:rows @captured)))))
