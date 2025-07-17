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
(ns metabase.driver.pinot
  "Pinot driver."
  (:require
   [cheshire.core :as json]
   [clj-http.client :as http]
   [metabase.driver :as driver]
   [metabase.driver.pinot.client :as pinot.client]
   [metabase.driver.pinot.execute :as pinot.execute]
   [metabase.driver.pinot.query-processor :as pinot.qp]
   [metabase.driver.pinot.sync :as pinot.sync]
   [metabase.util.log :as log]
   [metabase.driver.sql-jdbc.connection.ssh-tunnel :as ssh]))

(driver/register! :pinot)

(doseq [[feature supported?] {:expression-aggregations        true
                              :schemas                        false
                              :set-timezone                   true
                              :temporal/requires-default-unit true}]
  (defmethod driver/database-supports? [:pinot feature] [_driver _feature _db] supported?))

(defmethod driver/can-connect? :pinot
  [_ details]
  {:pre [(map? details)]}
  (ssh/with-ssh-tunnel [details-with-tunnel details]
    (let [{:keys [auth-enabled auth-token-type auth-token-value database-name]} details
          headers (if auth-enabled
                    {"Authorization" (str auth-token-type " " auth-token-value)
                     "database" database-name} ;; Create the Authorization header
                    {})]
      ;; Make the GET request with headers properly nested
      (= 200 (:status (http/get (pinot.client/details->url details-with-tunnel "/health")
                                {:headers headers})))))) ;; The headers are now in a :headers map


(defmethod driver/describe-table :pinot
  [_ database table]
  (pinot.sync/describe-table database table))

(defmethod driver/dbms-version :pinot
  [_ database]
  (pinot.sync/dbms-version database))

(defmethod driver/describe-database :pinot
  [_ database]
  (pinot.sync/describe-database database))

(defmethod driver/mbql->native :pinot
  [_ query]
  (pinot.qp/mbql->native query))

(defn- truncate-for-logging
  "Safely truncate query content for logging to avoid exposing sensitive data and reduce log verbosity."
  [query-str max-length]
  (if (and query-str (> (count query-str) max-length))
    (str (subs query-str 0 max-length) "... (truncated)")
    query-str))

(defn- add-timeout-to-query [query timeout]
  (log/debugf "add-timeout-to-query called with query type: %s, timeout: %s" 
              (type query) timeout)
  (cond
    ;; If it's not a string, assume it's already a map/object
    (not (string? query))
    (do
      (log/debugf "Query is not a string, adding timeout to existing map")
      (assoc-in query [:queryOptions :timeoutMs] timeout))

    ;; If it's a string that looks like JSON (starts with { or [)
    :else
    (let [trimmed-query (.trim query)]
      (if (or (.startsWith trimmed-query "{")
              (.startsWith trimmed-query "["))
        (do
          (log/debugf "Query appears to be JSON, attempting to parse")
          (try
            (let [parsed (json/parse-string query keyword)
                  result (assoc-in parsed [:queryOptions :timeoutMs] timeout)]
              (log/debugf "Successfully parsed JSON and added timeout")
              result)
            (catch Exception e
              (log/warnf e "Failed to parse query as JSON, treating as SQL string: %s"
                         (truncate-for-logging trimmed-query 100))
              ;; Fall back to treating it as a SQL string
              {:sql query :queryOptions {:timeoutMs timeout}})))

        ;; If it's a raw SQL string, wrap it in proper Pinot query JSON structure
        (do
          (log/debugf "Query is SQL string, wrapping in JSON structure: %s"
                      (truncate-for-logging trimmed-query 50))
          {:sql query :queryOptions {:timeoutMs timeout}})))))

(defmethod driver/execute-reducible-query :pinot
  [_driver query context respond]
   (log/debugf "Executing reducible Pinot query: %s" query)

  (pinot.execute/execute-reducible-query
   (partial pinot.client/do-query-with-cancellation (:canceled-chan context))
   (update-in query [:native :query] add-timeout-to-query 30000) ; 30 second default timeout
   respond))

(defmethod driver/db-start-of-week :pinot
  [_]
  :sunday) 