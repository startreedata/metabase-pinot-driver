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
   [metabase.driver.common :as driver.common]
   [metabase.driver.pinot.client :as pinot.client]
   [metabase.driver.pinot.execute :as pinot.execute]
   [metabase.driver.pinot.query-processor :as pinot.qp]
   [metabase.driver.pinot.sync :as pinot.sync]
   [metabase.util.i18n :refer [deferred-tru]]
   [metabase.util.log :as log]
   [metabase.driver.sql-jdbc.connection.ssh-tunnel :as ssh]))

(driver/register! :pinot)

(doseq [[feature supported?] {:expression-aggregations        true
                              :schemas                        false
                              :set-timezone                   true
                              :temporal/requires-default-unit true}]
  (defmethod driver/database-supports? [:pinot feature] [_driver _feature _db] supported?))

(defmethod driver/connection-properties :pinot
  [_driver]
  [{:name         "controller-endpoint"
    :display-name (deferred-tru "Controller Endpoint")
    :helper-text  (deferred-tru "The full URL of your Pinot Controller (e.g. http://localhost:9000)")
    :placeholder  "http://localhost:9000"
    :required     true}
   {:name         "auth-enabled"
    :display-name (deferred-tru "Authentication Enabled")
    :type         :boolean
    :default      false}
   {:name         "auth-token-type"
    :display-name (deferred-tru "Authentication Token Type")
    :placeholder  "Basic"
    :default      "Basic"}
   {:name         "auth-token-value"
    :display-name (deferred-tru "Authentication Token Value")
    :type         :password
    :placeholder  "••••••••"}
   {:name         "database-name"
    :display-name (deferred-tru "Database Name")
    :placeholder  "pinot"}
   {:name         "query-options"
    :display-name (deferred-tru "Query Options")
    :helper-text  (deferred-tru "Additional query options as key=value pairs separated by semicolons")
    :placeholder  "timeoutMs=30000;maxServerResponseSizeBytes=1048576"
    :type         :text}
   driver.common/ssh-tunnel-preferences])

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

(defn- add-timeout-to-query [query timeout]
  (let [parsed (if (string? query)
                 (json/parse-string query keyword)
                 query)]
    (assoc-in parsed [:queryOptions :timeoutMs] timeout)))

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