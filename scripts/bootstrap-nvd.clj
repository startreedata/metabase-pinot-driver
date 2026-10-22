;; Run from drivers/pinot with the pinned :security-scan classpath.
(require '[clojure.edn :as edn]
         '[clojure.java.io :as io]
         '[nvd.config :as nvd])
(import '(java.time Duration Instant ZonedDateTime)
        '(java.util Properties)
        '(org.owasp.dependencycheck Engine))

(def feed "https://dependency-check.github.io/DependencyCheck_Builder/nvd_cache/")

(defn recent! [timestamp]
  (when-not (and timestamp
                 (not (.isAfter (.toInstant timestamp) (Instant/now)))
                 (< (.getSeconds (Duration/between (.toInstant timestamp) (Instant/now)))
                    (* 120 24 60 60)))
    (throw (ex-info "NVD boundary must be in the past and less than 120 days old" {})))
  timestamp)

(let [config (edn/read-string (slurp "nvd-clojure.edn"))
      data-dir (io/file (:data-directory config))
      cold? (zero? (.length (io/file data-dir "odc.mv.db")))
      seed-config (io/file data-dir "bootstrap.edn")]
  (.mkdirs data-dir)
  ;; An explicit empty key prevents nvd-clojure's environment-key fallback.
  (spit seed-config (pr-str (cond-> (assoc-in config [:nvd-api :key] "")
                             cold? (assoc-in [:nvd-api :datafeed :url]
                                             (str feed "nvdcve-{0}.json.gz")))))
  (try
    (nvd/with-config [project (.getPath seed-config)]
      (let [^Engine engine (:engine project)]
        (if cold?
          (let [properties (Properties.)]
            (with-open [reader (io/reader (str feed "cache.properties"))]
              (.load properties reader))
            (recent! (ZonedDateTime/parse (.getProperty properties "lastModifiedDate")))
            (println "Bootstrapping NVD from the maintainer's bulk JSON feed")
            (.doUpdates engine true))
          (do (println "Using the restored NVD database")
              (.openDatabase engine)))
        (let [boundary (recent! (.. engine getDatabase getDatabaseProperties
                                   (getTimestamp "nvd.api.last.modified")))]
          (spit "target/nvd-boundary.txt" (str (.toEpochSecond boundary)))
          (println "NVD delta boundary:" (str boundary)))))
    (finally (.delete seed-config))))
