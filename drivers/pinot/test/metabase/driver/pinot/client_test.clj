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
(ns metabase.driver.pinot.client-test
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer [deftest is testing]]
   [metabase.driver.pinot.client :as client]
   [metabase.driver.sql-jdbc.connection.ssh-tunnel :as ssh]
   [metabase.secrets.models.secret :as secret]))

(deftest details->url-test
  (is (= "http://localhost:9000/health"
         (client/details->url {:controller-endpoint "http://localhost:9000"} "/health"))))

(deftest parse-value-handles-nil
  (is (nil? (client/parse-value nil))))

(deftest query-options-parsing-and-mapping-test
  (testing "round trip between string and map query option formats"
    (let [options-str "timeoutMs=1000;useMultistageEngine=true;threshold=0.5"
          parsed (client/parse-query-options options-str)
          roundtripped (client/parse-query-options (client/map->query-options parsed))]
      (is (= 1000 (:timeoutMs parsed)))
      (is (= true (:useMultistageEngine parsed)))
      (is (== 0.5 (:threshold parsed)))
      (is (= (:timeoutMs parsed) (:timeoutMs roundtripped)))
      (is (= (:useMultistageEngine parsed) (:useMultistageEngine roundtripped)))
      (is (== (:threshold parsed) (:threshold roundtripped))))
    ))

(deftest parse-query-options-handles-empty-input
  (is (= {} (client/parse-query-options nil)))
  (is (= {:flag false}
         (client/parse-query-options (client/map->query-options {:flag false})))))

(deftest do-query-merges-query-options
  (testing "query options from details merge into the outgoing body"
    (let [captured (atom nil)
          details {:controller-endpoint "http://pinot:9000"
                   :database-name "analytics"
                   :auth-enabled true
                   :auth-token-type "Bearer"
                   :auth-token-value "secret"
                   :query-options "timeoutMs=1000;useMultistageEngine=true"}
          query {:sql "SELECT 1" :queryOptions {:existing "foo"}}]
      (with-redefs [ssh/do-with-ssh-tunnel (fn [config f] (f config))
                   secret/value-as-string (fn [_ details key]
                                             (get details (keyword key)))
                   client/POST (fn [url & {:as opts}]
                                 (reset! captured {:url url :opts opts})
                                 {:status 200 :body "{}"})]
        (is (= {:status 200 :body "{}"}
               (client/do-query details query)))
        (is (= "http://pinot:9000/sql" (:url @captured)))
        (let [body (:body (:opts @captured))
              parsed-body (client/parse-query-options (:queryOptions body))]
          (is (= {:existing "foo"
                  :timeoutMs 1000
                  :useMultistageEngine true}
                 parsed-body)))
        (is (= "analytics" (:database-name (:opts @captured))))
        (is (= "Bearer" (:auth-token-type (:opts @captured))))
        (is (= "secret" (:auth-token-value (:opts @captured))))))))

(deftest do-request-adds-headers-and-parses-json
  (testing "JSON responses are parsed and headers constructed"
    (let [captured (atom nil)
          result (#'client/do-request
                  (fn [url opts]
                    (reset! captured {:url url :opts opts})
                    {:status 200 :body "{\"value\":123}"})
                  "http://pinot:9000/test"
                  :auth-enabled true
                  :auth-token-type "Bearer"
                  :auth-token-value "secret"
                  :database-name "analytics")]
      (is (= {:value 123} result))
      (is (= "http://pinot:9000/test" (:url @captured)))
      (is (= "application/json;charset=UTF-8" (get-in @captured [:opts :headers "Content-Type"])))
      (is (= "Bearer secret" (get-in @captured [:opts :headers "Authorization"])))
      (is (= "analytics" (get-in @captured [:opts :headers "database"])))))

  (testing "text responses skip JSON parsing"
    (let [result (#'client/do-request (fn [_ _] {:status 200 :body "OK"})
                    "http://pinot:9000/health"
                    :as :text)]
      (is (= "OK" result))))

  (testing "non-200 responses raise an error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Pinot request error"
                          (#'client/do-request
                           (fn [_ _] {:status 500 :body "{}"})
                           "http://pinot:9000/error"))))

  (testing "malformed JSON responses surface parsing errors"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Failed to parse Pinot response body"
                          (#'client/do-request
                           (fn [_ _] {:status 200 :body "{not-json}"})
                           "http://pinot:9000/bad-json"))))

  (testing "exceptions include parsed response bodies"
    (try
      (#'client/do-request
       (fn [_ _]
         (throw (ex-info "boom" {:body "{\"errorMessage\":\"bad\"}"})))
       "http://pinot:9000/broken")
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)]
          (is (= "bad" (.getMessage e)))
          (is (= {:errorMessage "bad"} (:response data)))
          (is (= "http://pinot:9000/broken" (:request-url data)))
          (is (:request-options data)))))))

(deftest cancel-query-with-id!-test
  (testing "missing query-id is a no-op"
    (is (nil? (#'client/cancel-query-with-id! {} nil))))

  (testing "when a query id is provided a DELETE request is sent"
    (let [captured (atom nil)
          details {:controller-endpoint "http://pinot:9000"
                   :database-name "analytics"
                   :auth-enabled true
                   :auth-token-type "Bearer"
                   :auth-token-value "secret"}]
      (with-redefs [ssh/do-with-ssh-tunnel (fn [config f] (f config))
                    secret/value-as-string (fn [_ det _] (:auth-token-value det))
                    client/DELETE (fn [url & {:as opts}]
                                    (reset! captured {:url url :opts opts}))]
        (#'client/cancel-query-with-id! details "abc123")
        (is (= "http://pinot:9000/pinot/v2/abc123" (:url @captured)))
        (is (= "analytics" (:database-name (:opts @captured))))
        (is (= "Bearer" (:auth-token-type (:opts @captured))))
        (is (= "secret" (:auth-token-value (:opts @captured))))))))

(deftest do-query-with-cancellation-triggers-cancel-hook
  (testing "cancellation hook still runs after query completes"
    (let [cancelled (atom nil)
          cancel-chan (async/chan)]
      (with-redefs [client/do-query (fn [_ _] :done)
                    client/cancel-query-with-id! (fn [_ query-id] (reset! cancelled query-id))]
        (let [result (client/do-query-with-cancellation cancel-chan {:details {}} {:context {:queryId "q1"}})]
          ;; send cancel signal after query completes to exercise go-loop
          (async/>!! cancel-chan :cancel)
          (Thread/sleep 10)
          (is (= :done result))
          (is (= "q1" @cancelled)))))))

(deftest do-query-with-cancellation-success
  (testing "successful queries return the result without canceling"
    (with-redefs [client/do-query (fn [_ _] {:result 42})
                  client/cancel-query-with-id! (fn [& _])]
      (let [canceled-chan (async/chan)
            result (client/do-query-with-cancellation canceled-chan {:details true} {:context {:queryId "abc"}})]
        (is (= {:result 42} result))))))
