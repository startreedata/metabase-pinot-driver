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
(ns metabase.driver.pinot.sync
  (:require
   [metabase.driver.pinot.client :as pinot.client]

   [metabase.driver.sql-jdbc.connection.ssh-tunnel :as ssh]
   [clojure.string :as str]))

(defn- pinot-type->base-type [field-type]
  (case field-type
    "STRING"      :type/Text
    "FLOAT"       :type/Float
    "DOUBLE"      :type/Float
    "LONG"        :type/Integer
    "INT"         :type/Integer
    "TIMESTAMP"   :type/Time
    :type/Float))

(defn describe-table
  "Impl of `driver/describe-table` for Pinot."
  [database table]
  {:pre [(map? database) (map? table)]}
  (ssh/with-ssh-tunnel [details-with-tunnel (:details database)]
    (let [request-path (str "/tables/" (table :name) "/schema")
          response (pinot.client/GET (pinot.client/details->url details-with-tunnel request-path)
                     :database-name (:database-name details-with-tunnel)
                     :auth-enabled        (-> database :details :auth-enabled)
                     :auth-token-type     (-> database :details :auth-token-type)
                     :auth-token-value    (-> database :details :auth-token-value))
          dimensions    (response :dimensionFieldSpecs)
          metrics       (response :metricFieldSpecs)
          time-columns  (response :dateTimeFieldSpecs)]
      {:schema nil
       :name   (:name table)
       :fields (set (mapcat (fn [field-group]
                              (map-indexed (fn [idx field]
                                             (let [base {:name              (name (field :name))
                                                         :base-type         (pinot-type->base-type (field :dataType))
                                                         :database-type     (field :dataType)
                                                         :database-position (inc idx)}]
                                               (if (contains? field :format)
                                                 ;; Add extra info for time columns
                                                 (assoc base
                                                        :format      (field :format)
                                                        :granularity (field :granularity))
                                                 base)))
                                           field-group))
                            [dimensions metrics time-columns]))})))

(defn describe-database
  "Impl of `driver/describe-database` for Pinot."
  [database]
  {:pre [(map? (:details database))]}
  (ssh/with-ssh-tunnel [details-with-tunnel (:details database)]
    (let [response (pinot.client/GET (pinot.client/details->url details-with-tunnel "/tables")
                     :database-name (:database-name details-with-tunnel)
                     :auth-enabled     (-> database :details :auth-enabled)
                     :auth-token-type    (-> database :details :auth-token-type)
                     :auth-token-value (-> database :details :auth-token-value))
          pinot-tables (response :tables)]
      {:tables (set (for [table-name pinot-tables]
                      {:schema nil, :name (str/replace table-name #"^.*\." "")}))})))


(defn dbms-version
  "Impl of `driver/dbms-version` for Pinot."
  [database]
  {:pre [(map? (:details database))]}
  (ssh/with-ssh-tunnel [details-with-tunnel (:details database)]
    (if (:database-name details-with-tunnel)
      ;; If database-name is configured, check health endpoint
      (let [response (pinot.client/GET (pinot.client/details->url details-with-tunnel "/health")
                       :auth-enabled     (-> database :details :auth-enabled)
                       :auth-token-type  (-> database :details :auth-token-type)
                       :auth-token-value (-> database :details :auth-token-value)
                       :as :text)]
        (if (= "OK" response)
          {:version "latest"}
          {:version "unknown"}))
      ;; If no database-name, just get version
      (let [response (pinot.client/GET (pinot.client/details->url details-with-tunnel "/version")
                       :auth-enabled     (-> database :details :auth-enabled)
                       :auth-token-type  (-> database :details :auth-token-type)
                       :auth-token-value (-> database :details :auth-token-value))
            version (response :pinot-segment-uploader-default)]
        {:version version})))) 