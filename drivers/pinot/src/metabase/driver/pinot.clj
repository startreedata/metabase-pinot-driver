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
(log/debugf "*** PINOT DRIVER: Pinot driver registered successfully!")
(println "*** PINOT DRIVER: Pinot driver registered successfully!") ; Also print to stdout

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
  (log/debugf "substitute-param-value called with param-value: %s (type: %s)" 
              (u/pprint-to-str param-value) (type param-value))
  
  (cond
    ;; Handle no-value case
    (= params/no-value param-value)
    (do
      (log/debugf "Handling no-value case, throwing error to inform user")
      (throw (ex-info "No values selected for parameter. Please select at least one value."
                      {:type :metabase.driver/parameter-no-values})))
    
    ;; Handle single-element arrays - return just the value without parentheses
    ;; This allows single values to work with both = and IN operators
    ;; Example: [19393] -> "19393" (can be used in "WHERE id = 19393" or "WHERE id IN (19393)")
    (and (sequential? param-value) (= 1 (count param-value)))
    (let [first-value (first param-value)]
      (log/debugf "Handling single-element array: %s" first-value)
      (cond
        (string? first-value)
        (str "'" (str/replace first-value "'" "''") "'")
        (number? first-value)
        (str first-value)
        (boolean? first-value)
        (str first-value)
        :else
        (str "'" (str first-value) "'")))
    
    ;; Handle multi-element arrays - wrap values in parentheses for IN clause
    ;; Multiple values must be wrapped in parentheses for SQL IN operator
    ;; Example: [19393, 19690] -> "(19393, 19690)" (for "WHERE id IN (19393, 19690)")
    (and (sequential? param-value) (> (count param-value) 1))
    (let [values (map (fn [v]
                        (cond
                          (string? v) (str "'" (str/replace v "'" "''") "'")
                          (number? v) (str v)
                          (boolean? v) (str v)
                          :else (str "'" (str v) "'")))
                      param-value)]
      (log/debugf "Handling multi-element array with %d values: %s" (count param-value) values)
      (str "(" (str/join ", " values) ")"))
    
    ;; Handle string values - escape single quotes
    (string? param-value)
    (do
      (log/debugf "Handling string value: %s" param-value)
      (str "'" (str/replace param-value "'" "''") "'"))
    
    ;; Handle numbers
    (number? param-value)
    (do
      (log/debugf "Handling number value: %s" param-value)
      (str param-value))
    
    ;; Handle booleans
    (boolean? param-value)
    (do
      (log/debugf "Handling boolean value: %s" param-value)
      (str param-value))
    
    ;; Handle dates
    (instance? Date param-value)
    (do
      (log/debugf "Handling date value: %s" (:s param-value))
      (str "'" (:s param-value) "'"))
    
    ;; Handle field filters - these should be handled differently
    (params/FieldFilter? param-value)
    (let [{:keys [field value]} param-value]
      (log/debugf "Handling FieldFilter - field: %s, value: %s (type: %s)" 
                  (u/pprint-to-str field) (u/pprint-to-str value) (type value))
      (if (= params/no-value value)
        (do
          (log/debugf "FieldFilter has no-value, throwing error to inform user")
          (throw (ex-info "No values selected for field filter. Please select at least one value."
                          {:type :metabase.driver/field-filter-no-values
                           :field-name (:name field)
                           :field-id (:id field)})))
        (let [field-name (:name field)
              ;; Extract the actual values and operator type from the FieldFilter value structure
              actual-values (if (and (map? value) (contains? value :value))
                              (:value value)  ; FieldFilter value is a map with :value key
                              value)          ; FieldFilter value is the actual value
              operator-type (if (and (map? value) (contains? value :type))
                             (:type value)    ; Extract operator type from FieldFilter value
                             :string/=)       ; Default to equality
              case-sensitive (if (and (map? value) (contains? value :options))
                              (get-in value [:options :case-sensitive])
                              false)          ; Default to case-insensitive
              field-value (cond
                           ;; Handle arrays in field filters
                           (and (sequential? actual-values) (= 1 (count actual-values)))
                           (let [first-value (first actual-values)]
                             (log/debugf "FieldFilter single-element array: %s" first-value)
                             (cond
                               (string? first-value) (str "'" (str/replace first-value "'" "''") "'")
                               (number? first-value) (str first-value)
                               (boolean? first-value) (str first-value)
                               :else (str "'" (str first-value) "'")))
                           
                           (and (sequential? actual-values) (> (count actual-values) 1))
                           (let [values (map (fn [v]
                                               (cond
                                                 (string? v) (str "'" (str/replace v "'" "''") "'")
                                                 (number? v) (str v)
                                                 (boolean? v) (str v)
                                                 :else (str "'" (str v) "'")))
                                             actual-values)]
                             (log/debugf "FieldFilter multi-element array with %d values: %s" (count actual-values) values)
                             (str "(" (str/join ", " values) ")"))
                           
                           (string? actual-values) (do
                                                    (log/debugf "FieldFilter string value: %s" actual-values)
                                                    (str "'" (str/replace actual-values "'" "''") "'"))
                           (number? actual-values) (do
                                                    (log/debugf "FieldFilter number value: %s" actual-values)
                                                    (str actual-values))
                           (boolean? actual-values) (do
                                                     (log/debugf "FieldFilter boolean value: %s" actual-values)
                                                     (str actual-values))
                           :else (do
                                  (log/debugf "FieldFilter other value: %s" actual-values)
                                  (str "'" actual-values "'")))]
          (let [result (cond
                        ;; Handle nil values - throw error to inform user
                        (nil? actual-values)
                        (do
                          (log/debugf "FieldFilter has nil values, throwing error to inform user")
                          (throw (ex-info "No values selected for field filter. Please select at least one value."
                                          {:type :metabase.driver/field-filter-no-values
                                           :field-name field-name
                                           :field-id (:id field)})))
                        
                        ;; Handle empty arrays - throw error to inform user
                        (and (sequential? actual-values) (empty? actual-values))
                        (do
                          (log/debugf "FieldFilter has empty array, throwing error to inform user")
                          (throw (ex-info "No values selected for field filter. Please select at least one value."
                                          {:type :metabase.driver/field-filter-no-values
                                           :field-name field-name
                                           :field-id (:id field)})))
                        
                        ;; Handle different operator types
                        (= operator-type :string/contains)
                        (if (and (sequential? actual-values) (> (count actual-values) 1))
                          ;; Multiple values with contains - use OR with LIKE
                          (let [like-clauses (map (fn [v]
                                                    (let [escaped-value (str/replace (str v) "'" "''")
                                                          like-value (str "'%" escaped-value "%'")]
                                                      (str "\"" field-name "\" LIKE " like-value)))
                                                  actual-values)]
                            (str "(" (str/join " OR " like-clauses) ")"))
                          ;; Single value with contains - use LIKE
                          (let [escaped-value (str/replace (str (first actual-values)) "'" "''")
                                like-value (str "'%" escaped-value "%'")]
                            (str "\"" field-name "\" LIKE " like-value)))
                        
                        (= operator-type :string/does-not-contain)
                        (if (and (sequential? actual-values) (> (count actual-values) 1))
                          ;; Multiple values with does-not-contain - use AND with NOT LIKE
                          (let [not-like-clauses (map (fn [v]
                                                        (let [escaped-value (str/replace (str v) "'" "''")
                                                              like-value (str "'%" escaped-value "%'")]
                                                          (str "\"" field-name "\" NOT LIKE " like-value)))
                                                      actual-values)]
                            (str "(" (str/join " AND " not-like-clauses) ")"))
                          ;; Single value with does-not-contain - use NOT LIKE
                          (let [escaped-value (str/replace (str (first actual-values)) "'" "''")
                                like-value (str "'%" escaped-value "%'")]
                            (str "\"" field-name "\" NOT LIKE " like-value)))
                        
                        (= operator-type :string/starts-with)
                        (if (and (sequential? actual-values) (> (count actual-values) 1))
                          ;; Multiple values with starts-with - use OR with LIKE
                          (let [like-clauses (map (fn [v]
                                                    (let [escaped-value (str/replace (str v) "'" "''")
                                                          like-value (str "'" escaped-value "%'")]
                                                      (str "\"" field-name "\" LIKE " like-value)))
                                                  actual-values)]
                            (str "(" (str/join " OR " like-clauses) ")"))
                          ;; Single value with starts-with - use LIKE
                          (let [escaped-value (str/replace (str (first actual-values)) "'" "''")
                                like-value (str "'" escaped-value "%'")]
                            (str "\"" field-name "\" LIKE " like-value)))
                        
                        (= operator-type :string/ends-with)
                        (if (and (sequential? actual-values) (> (count actual-values) 1))
                          ;; Multiple values with ends-with - use OR with LIKE
                          (let [like-clauses (map (fn [v]
                                                    (let [escaped-value (str/replace (str v) "'" "''")
                                                          like-value (str "'%" escaped-value "'")]
                                                      (str "\"" field-name "\" LIKE " like-value)))
                                                  actual-values)]
                            (str "(" (str/join " OR " like-clauses) ")"))
                          ;; Single value with ends-with - use LIKE
                          (let [escaped-value (str/replace (str (first actual-values)) "'" "''")
                                like-value (str "'%" escaped-value "'")]
                            (str "\"" field-name "\" LIKE " like-value)))
                        
                        (= operator-type :string/!=)
                        (if (and (sequential? actual-values) (> (count actual-values) 1))
                          ;; Multiple values with not equal - use NOT IN
                          (str "\"" field-name "\" NOT IN " field-value)
                          ;; Single value with not equal - use !=
                          (str "\"" field-name "\" != " field-value))
                        
                        (= operator-type :string/=)
                        (if (and (sequential? actual-values) (> (count actual-values) 1))
                          ;; Multiple values with equal - use IN
                          (str "\"" field-name "\" IN " field-value)
                          ;; Single value with equal - use =
                          (str "\"" field-name "\" = " field-value))
                        
                        ;; Default case: equality or IN
                        :else
                        (if (and (sequential? actual-values) (> (count actual-values) 1))
                          ;; Multiple values - use IN
                          (str "\"" field-name "\" IN " field-value)
                          ;; Single value - use =
                          (str "\"" field-name "\" = " field-value)))]
            (log/debugf "FieldFilter result (operator: %s): %s" operator-type result)
            result))))
    
    ;; Handle referenced card queries
    (params/ReferencedCardQuery? param-value)
    (let [{:keys [query]} param-value]
      (log/debugf "Handling ReferencedCardQuery: %s" (u/pprint-to-str query))
      (if (string? query)
        query
        (json/generate-string query)))
    
    ;; Handle referenced query snippets
    (params/ReferencedQuerySnippet? param-value)
    (do
      (log/debugf "Handling ReferencedQuerySnippet: %s" (:content param-value))
      (:content param-value))
    
    ;; Default case - convert to string
    :else
    (do
      (log/debugf "Handling default case, converting to string: %s" param-value)
      (str "'" (str param-value) "'"))))

(defn- substitute-param
  "Substitute a single parameter in the parsed query."
  [param->value [sql args missing] in-optional? {:keys [k]}]
  (log/debugf "substitute-param called with key: %s, param->value: %s, sql: %s" 
              k (u/pprint-to-str param->value) sql)
  (if-not (contains? param->value k)
    (do
      (log/debugf "Parameter key %s not found in param->value" k)
      [sql args (conj missing k)])
    (let [v (get param->value k)]
      (log/debugf "Found parameter value for key %s: %s (type: %s)" k (u/pprint-to-str v) (type v))
      (cond
        (= params/no-value v)
        (do
          (log/debugf "Parameter has no-value, throwing error to inform user")
          (throw (ex-info "No values selected for parameter. Please select at least one value."
                          {:type :metabase.driver/parameter-no-values
                           :parameter-key k})))
        
        :else
        (let [v-type (type v)
              v-class (class v)
              is-sequential? (sequential? v)
              is-vector? (vector? v)
              is-list? (list? v)
              is-coll? (coll? v)
              _ (log/debugf "Parameter value type check for key %s: v=%s, type=%s, class=%s, sequential?=%s, vector?=%s, list?=%s, coll?=%s" 
                           k (u/pprint-to-str v) v-type v-class is-sequential? is-vector? is-list? is-coll?)
              substituted-value (substitute-param-value v)
              is-array? (sequential? v)
              array-count (if is-array? (count v) 0)
              is-single-value? (or (not is-array?) (= 1 array-count))
              _ (log/debugf "Array detection for key %s: is-array?=%s, array-count=%s, is-single-value?=%s" k is-array? array-count is-single-value?)
              ;; Check if SQL template has = operator before the parameter
              ;; Regex pattern breakdown: #"=\s*$"
              ;;   =     - matches literal equals sign
              ;;   \s*   - matches zero or more whitespace characters (spaces, tabs, newlines)
              ;;   $     - matches the end of the string
              ;; This detects if SQL ends with "=" (optionally followed by whitespace)
              ;; Example matches: "WHERE id =", "WHERE id = ", "WHERE id =\n"
              ;; Example non-matches: "WHERE id IN", "WHERE id", "WHERE id = 1"
              has-equals-before-param? (re-find #"=\s*$" sql)
              ;; Debug: Log the condition components
              _ (log/debugf "Parameter substitution debug for key %s: is-single-value=%s, has-equals=%s, sql-ending='%s'" 
                           k is-single-value? has-equals-before-param? 
                           (if (> (count sql) 20) (subs sql (- (count sql) 20)) sql))
              ;; For single values: if = operator, return value without parentheses; if IN, wrap in parentheses
              ;; For multiple values: always wrap in parentheses (already done by substitute-param-value)
              ;; Condition: single value AND no = operator (meaning IN operator) -> wrap in parentheses
              ;; Single values can be any type: Integer (19393), String ("Alabama"), Date ("2023-10-29"), etc.
              condition-result (and is-single-value? (not has-equals-before-param?))
              _ (log/debugf "Condition check: (and is-single-value? (not has-equals-before-param?)) = %s" condition-result)
              final-value (if condition-result
                            ;; Single value with IN operator - wrap in parentheses
                            ;; Handles any data type: Integer (19393), String ("Alabama"), Date ("2023-10-29"), etc.
                            ;; Also handles single-element arrays: [19393], ["Alabama"], etc.
                            ;; Example: "WHERE id IN {{param}}" with 19393 -> "WHERE id IN (19393)"
                            ;; Example: "WHERE state IN {{param}}" with "Alabama" -> "WHERE state IN ('Alabama')"
                            ;; Example: "WHERE date IN {{param}}" with "2023-10-29" -> "WHERE date IN ('2023-10-29')"
                            (let [wrapped-value (str "(" substituted-value ")")]
                              (log/debugf "Wrapping single value in parentheses: %s -> %s" substituted-value wrapped-value)
                              wrapped-value)
                            ;; Single value with = operator or multiple values - use as-is
                            ;; Example: "WHERE id = {{param}}" with 19393 -> "WHERE id = 19393"
                            ;; Example: "WHERE state = {{param}}" with "Alabama" -> "WHERE state = 'Alabama'"
                            (do
                              (log/debugf "Using substituted value as-is (no wrapping): %s" substituted-value)
                              substituted-value))
              final-sql (str sql final-value)]
          (log/debugf "Substituted value for key %s: %s (has-equals=%s), final-value=%s, final-sql: %s" 
                     k substituted-value has-equals-before-param? final-value final-sql)
          (if (empty? substituted-value)
            ;; If substituted value is empty, don't add anything to SQL
            [sql args missing]
            [final-sql args missing]))))))

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
  (log/debugf "substitute-parameters called with param->value: %s" (u/pprint-to-str param->value))
  (log/tracef "Substituting params for Pinot\n%s\nin query:\n%s" 
              (u/pprint-to-str param->value) 
              (u/pprint-to-str parsed-query))
  (let [[sql args missing] (try
                             (substitute* param->value parsed-query false)
                             (catch Throwable e
                               (log/errorf e "Error substituting parameters: %s" (ex-message e))
                               (throw (ex-info (str "Unable to substitute parameters: " (ex-message e))
                                               {:params param->value
                                                :parsed-query parsed-query}
                                               e))))]
    (log/debugf "Substituted SQL: %s" sql)
    (log/debugf "Substituted args: %s" args)
    (log/debugf "Missing parameters: %s" missing)
    (when (seq missing)
      (throw (ex-info (str "Cannot run the query: missing required parameters: " (set missing))
                      {:missing missing})))
    [sql args]))

(mu/defmethod driver/substitute-native-parameters :pinot
  [_driver {:keys [query] :as inner-query} :- [:and [:map-of :keyword :any] [:map {:query ::lib.schema.common/non-blank-string}]]]
  (log/debugf "*** PINOT DRIVER: substitute-native-parameters called with query: %s" query)
  (log/debugf "*** PINOT DRIVER: inner-query: %s" (u/pprint-to-str inner-query))
  (let [params-map          (params.values/query->params-map inner-query)
        referenced-card-ids (params.values/referenced-card-ids params-map)]
    (log/debugf "*** PINOT DRIVER: params-map: %s" (u/pprint-to-str params-map))
    (log/debugf "*** PINOT DRIVER: referenced-card-ids: %s" referenced-card-ids)
    (let [parsed-query (params.parse/parse query)]
      (log/debugf "*** PINOT DRIVER: parsed-query: %s" (u/pprint-to-str parsed-query))
      (let [[final-query params] (substitute-parameters parsed-query params-map)]
        (log/debugf "*** PINOT DRIVER: Parameter substitution result - Final Query: %s, Params: %s" final-query params)
        (cond-> (assoc inner-query
                       :query  final-query
                       :params params)
          (seq referenced-card-ids)
          (update :query-permissions/referenced-card-ids set/union referenced-card-ids))))))

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
   (log/debugf "*** PINOT DRIVER: Executing reducible Pinot query: %s" (u/pprint-to-str query))

  (pinot.execute/execute-reducible-query
   (partial pinot.client/do-query-with-cancellation (:canceled-chan context))
   (update-in query [:native :query] add-timeout-to-query 30000) ; 30 second default timeout
   respond))

(defmethod driver/db-start-of-week :pinot
  [_]
  :sunday) 