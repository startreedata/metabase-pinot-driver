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
   [clojure.set :as set]
   [clojure.string :as str]
   [metabase.driver :as driver]
   [metabase.driver.common.parameters :as params]
   [metabase.driver.common.parameters.parse :as params.parse]
   [metabase.driver.common.parameters.values :as params.values]
   [metabase.driver.pinot.client :as pinot.client]
   [metabase.driver.pinot.execute :as pinot.execute]
   [metabase.driver.pinot.query-processor :as pinot.qp]
   [metabase.driver.pinot.sync :as pinot.sync]
   [metabase.lib.schema.common :as lib.schema.common]
   [metabase.util :as u]
   [metabase.util.log :as log]
   [metabase.util.malli :as mu]
   [metabase.driver.sql-jdbc.connection.ssh-tunnel :as ssh])
  (:import
   (metabase.driver.common.parameters Date)))

(driver/register! :pinot)

(doseq [[feature supported?] {:expression-aggregations        true
                              :schemas                        false
                              :set-timezone                   true
                              :temporal/requires-default-unit true
                              :native-parameters              true}]
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

(defn- substitute-param-value
  "Substitute a parameter value in Pinot query format.
   For Pinot, we need to handle different parameter types appropriately."
  [param-value]
  (cond
    ;; Handle no-value case
    (= params/no-value param-value)
    "1 = 1"
    
    ;; Handle arrays - extract the first value if it's a single-element array
    (and (sequential? param-value) (= 1 (count param-value)))
    (let [first-value (first param-value)]
      (cond
        (string? first-value)
        (str "'" (str/replace first-value "'" "''") "'")
        (number? first-value)
        (str first-value)
        (boolean? first-value)
        (str first-value)
        :else
        (str "'" (str first-value) "'")))
    
    ;; Handle multi-element arrays - create IN clause
    (and (sequential? param-value) (> (count param-value) 1))
    (let [values (map (fn [v]
                        (cond
                          (string? v) (str "'" (str/replace v "'" "''") "'")
                          (number? v) (str v)
                          (boolean? v) (str v)
                          :else (str "'" (str v) "'")))
                      param-value)]
      (str "(" (str/join ", " values) ")"))
    
    ;; Handle string values - escape single quotes
    (string? param-value)
    (str "'" (str/replace param-value "'" "''") "'")
    
    ;; Handle numbers
    (number? param-value)
    (str param-value)
    
    ;; Handle booleans
    (boolean? param-value)
    (str param-value)
    
    ;; Handle dates
    (instance? Date param-value)
    (str "'" (:s param-value) "'")
    
    ;; Handle field filters - these should be handled differently
    (params/FieldFilter? param-value)
    (let [{:keys [field value]} param-value]
      (if (= params/no-value value)
        "1 = 1"
        (let [field-name (:name field)
              field-value (cond
                           ;; Handle arrays in field filters
                           (and (sequential? value) (= 1 (count value)))
                           (let [first-value (first value)]
                             (cond
                               (string? first-value) (str "'" (str/replace first-value "'" "''") "'")
                               (number? first-value) (str first-value)
                               (boolean? first-value) (str first-value)
                               :else (str "'" (str first-value) "'")))
                           
                           (and (sequential? value) (> (count value) 1))
                           (let [values (map (fn [v]
                                               (cond
                                                 (string? v) (str "'" (str/replace v "'" "''") "'")
                                                 (number? v) (str v)
                                                 (boolean? v) (str v)
                                                 :else (str "'" (str v) "'")))
                                             value)]
                             (str "(" (str/join ", " values) ")"))
                           
                           (string? value) (str "'" (str/replace value "'" "''") "'")
                           (number? value) (str value)
                           (boolean? value) (str value)
                           :else (str "'" value "'"))]
          (if (and (sequential? value) (> (count value) 1))
            (str "\"" field-name "\" IN " field-value)
            (str "\"" field-name "\" = " field-value)))))
    
    ;; Handle referenced card queries
    (params/ReferencedCardQuery? param-value)
    (let [{:keys [query]} param-value]
      (if (string? query)
        query
        (json/generate-string query)))
    
    ;; Handle referenced query snippets
    (params/ReferencedQuerySnippet? param-value)
    (:content param-value)
    
    ;; Default case - convert to string
    :else
    (str "'" (str param-value) "'")))

(defn- substitute-param
  "Substitute a single parameter in the parsed query."
  [param->value [sql args missing] in-optional? {:keys [k]}]
  (if-not (contains? param->value k)
    [sql args (conj missing k)]
    (let [v (get param->value k)]
      (cond
        (= params/no-value v)
        (if in-optional?
          [sql args (conj missing k)]
          [(str sql " 1 = 1") args missing])
        
        :else
        (let [substituted-value (substitute-param-value v)]
          [(str sql substituted-value) args missing])))))

(declare substitute*)

(defn- substitute-optional
  "Substitute an optional parameter clause."
  [param->value [sql args missing] {subclauses :args}]
  (let [[opt-sql opt-args opt-missing] (substitute* param->value subclauses true)]
    (if (seq opt-missing)
      [sql args missing]
      [(str sql opt-sql) (concat args opt-args) missing])))

(defn- substitute*
  "Recursively substitute parameters in parsed query."
  [param->value parsed in-optional?]
  (reduce
   (fn [[sql args missing] x]
     (cond
       (string? x)
       [(str sql x) args missing]
       
       (params/Param? x)
       (substitute-param param->value [sql args missing] in-optional? x)
       
       (params/Optional? x)
       (substitute-optional param->value [sql args missing] x)))
   ["" [] #{}]
   parsed))

(defn- substitute-parameters
  "Substitute parameters in a parsed query for Pinot."
  [parsed-query param->value]
  (log/tracef "Substituting params for Pinot\n%s\nin query:\n%s" 
              (u/pprint-to-str param->value) 
              (u/pprint-to-str parsed-query))
  (let [[sql args missing] (try
                             (substitute* param->value parsed-query false)
                             (catch Throwable e
                               (throw (ex-info (str "Unable to substitute parameters: " (ex-message e))
                                               {:params param->value
                                                :parsed-query parsed-query}
                                               e))))]
    (log/tracef "Substituted SQL: %s" sql)
    (when (seq missing)
      (throw (ex-info (str "Cannot run the query: missing required parameters: " (set missing))
                      {:missing missing})))
    [sql args]))

(mu/defmethod driver/substitute-native-parameters :pinot
  [_driver {:keys [query] :as inner-query} :- [:and [:map-of :keyword :any] [:map {:query ::lib.schema.common/non-blank-string}]]]
  (log/debugf "Substituting native parameters for Pinot driver. Query: %s" query)
  (let [params-map          (params.values/query->params-map inner-query)
        referenced-card-ids (params.values/referenced-card-ids params-map)
        [query params]      (-> query
                                params.parse/parse
                                (substitute-parameters params-map))]
    (log/debugf "Parameter substitution result - Query: %s, Params: %s" query params)
    (cond-> (assoc inner-query
                   :query  query
                   :params params)
      (seq referenced-card-ids)
      (update :query-permissions/referenced-card-ids set/union referenced-card-ids))))

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