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
   [clojure.set :as set]
   [clojure.string :as str]
   [cheshire.core :as json]
   [clj-http.client :as http]
   [metabase.driver :as driver]
   [metabase.driver.pinot.client :as pinot.client]
   [metabase.driver.pinot.execute :as pinot.execute]
   [metabase.driver.pinot.query-processor :as pinot.qp]
   [metabase.driver.pinot.sync :as pinot.sync]
   [metabase.driver.sql-jdbc.connection.ssh-tunnel :as ssh]
   [metabase.driver.sql.parameters.substitute :as sql.params.substitute]
   [metabase.driver.sql.parameters.substitution :as sql.params.substitution]
   [metabase.lib.core :as lib]
   [metabase.query-processor.parameters.values :as params.values]
   [metabase.util.date-2 :as u.date]
   [metabase.util.log :as log]))

(driver/register! :pinot)

(defmethod driver/connection-hosts :pinot
  [_driver {:keys [controller-endpoint tunnel-enabled] :as details}]
  (let [uri (try
              (when (string? controller-endpoint)
                (.parseServerAuthority (java.net.URI. controller-endpoint)))
              (catch java.net.URISyntaxException _ nil))
        host (some-> uri .getHost)]
    (when-not (and (#{"http" "https"} (some-> uri .getScheme str/lower-case))
                   (not (str/blank? host)))
      ;; URI exceptions include the input, which may contain credentials. Keep it out of the policy error's cause.
      (throw (ex-info "Invalid Pinot controller endpoint" {})))
    (let [hosts [(str/replace host #"^\[|\]$" "")]]
      ;; The SSH helper rewrites :host/:port, not :controller-endpoint, so the controller is still contacted directly.
      (if tunnel-enabled
        (let [tunnel-hosts (driver/hosts-from-details details [:tunnel-host])]
          (when (empty? tunnel-hosts)
            (throw (ex-info "Missing Pinot SSH tunnel host" {})))
          (into hosts tunnel-hosts))
        hosts))))

(doseq [[feature supported?] {:expression-aggregations        true
                              :schemas                        false
                              :set-timezone                   true
                              :native-parameters              true
                              :temporal/requires-default-unit true}]
  (defmethod driver/database-supports? [:pinot feature] [_driver _feature _db] supported?))

(defmethod driver/can-connect? :pinot
  [_ details]
  {:pre [(map? details)]}
  (ssh/with-ssh-tunnel [details-with-tunnel details]
    (let [{:keys [auth-enabled auth-token-type database-name]} details
          headers (if auth-enabled
                    {"Authorization" (str auth-token-type " " (pinot.client/auth-token details))
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

(defn- pinot-literal
  "Inline a parameter value directly into a Pinot SQL string."
  [v]
  (cond
    (nil? v) "NULL"
    (string? v) (str "'" (str/replace v #"'" "''") "'")
    (instance? java.util.UUID v) (str "'" v "'")
    (instance? java.time.temporal.Temporal v) (str "'" (u.date/format v) "'")
    (instance? java.util.Date v) (str "'" (u.date/format (.toInstant ^java.util.Date v)) "'")
    (boolean? v) (if v "TRUE" "FALSE")
    :else (str v)))

(defn- inline-params
  "Inline generated placeholders, leaving quoted SQL and comments intact."
  [sql params]
  (let [remaining (volatile! (seq params))]
    (str/replace sql #"(?s)'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|`(?:``|[^`])*`|--[^\r\n]*|/\*.*?\*/|\?"
                 (fn [token]
                   (if (and (= token "?") (seq @remaining))
                     (let [value (first @remaining)]
                       (vswap! remaining next)
                       (pinot-literal value))
                     token)))))

;; Reuse SQL parameter parsing without inheriting the SQL driver's dialect or feature set. Inline each generated
;; snippet before it is inserted into the user's SQL, because Pinot's HTTP API has no prepared-statement arguments.
(doseq [dispatch-type [nil Object
                      :metabase.lib.parameters.parse.types/date
                      :metabase.lib.parameters.parse.types/date-range
                      :metabase.lib.parameters.parse.types/date-time-range
                      :metabase.lib.parameters.parse.types/field-filter
                      :metabase.lib.parameters.parse.types/temporal-unit
                      :metabase.lib.parameters.parse.types/referenced-card-query
                      :metabase.lib.parameters.parse.types/referenced-table-query
                      :metabase.lib.parameters.parse.types/referenced-query-snippet]]
  (.addMethod sql.params.substitution/->replacement-snippet-info
              [:pinot dispatch-type]
              (fn [_driver value & args]
                (let [{:keys [replacement-snippet prepared-statement-args]}
                      (apply sql.params.substitution/->replacement-snippet-info :sql value args)]
                  {:replacement-snippet (inline-params replacement-snippet prepared-statement-args)
                   :prepared-statement-args []}))))

(defn- substitute-parameter-stage
  [metadata-providerable {native-query :native :as stage}]
  (let [params-map          (params.values/stage->params-map metadata-providerable stage)
        referenced-card-ids (params.values/referenced-card-ids params-map)
        [sql params]        (sql.params.substitute/substitute metadata-providerable (lib/parse-parameters native-query) params-map)]
    (cond-> (assoc stage :native sql :params params)
      (seq referenced-card-ids)
      (update :query-permissions/referenced-card-ids set/union referenced-card-ids))))

(defmethod driver/substitute-native-parameters-in-stage-method :pinot
  [_ query-or-metadata stage-or-number]
  ;; Metabase 0.63 passes metadata and a stage; 0.64 passes the whole query and a stage number.
  (if (map? stage-or-number)
    (substitute-parameter-stage query-or-metadata stage-or-number)
    (lib/update-query-stage query-or-metadata stage-or-number
                            (partial substitute-parameter-stage query-or-metadata))))

(defn- truncate-for-logging
  "Safely truncate query content for logging to avoid exposing sensitive data and reduce log verbosity."
  [query-str max-length]
  (if (and query-str (> (count query-str) max-length))
    (str (subs query-str 0 max-length) "... (truncated)")
    query-str))

(defn- add-timeout-to-query [query timeout]
  (log/debugf "add-timeout-to-query called with query type: %s, timeout: %s" (type query) timeout)
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
          (log/debugf "Query is SQL string, wrapping in JSON structure: %s" (truncate-for-logging trimmed-query 50))
          {:sql query :queryOptions {:timeoutMs timeout}})))))

(defmethod driver/execute-reducible-query :pinot
  [_driver query context respond]
   (log/debugf "Executing reducible Pinot query: %s" query)

  (pinot.execute/execute-reducible-query
   (partial pinot.client/do-query-with-cancellation (:canceled-chan context))
   (update-in query [:qp/compiled :query] add-timeout-to-query 30000) ; 30 second default timeout
   respond))

(defmethod driver/db-start-of-week :pinot
  [_]
  :sunday)
