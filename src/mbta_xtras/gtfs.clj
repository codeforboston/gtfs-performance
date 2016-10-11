(ns mbta-xtras.gtfs
  (:require [environ.core :refer [env]]
            [clojure.data.csv :refer [read-csv]]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def gtfs-path "resources/MBTA_GTFS.zip")

(def feed-info-url
  (env :feed-info-url
       "http://www.mbta.com/uploadedfiles/feed_info.txt"))

(def manifest-url
  "The URL for the zip file containing the full trip information."
  (env :manifest-url
       "http://www.mbta.com/uploadedfiles/MBTA_GTFS.zip"))

(defn download-zip
  "Saves the GTFS file specified in the environment to file and returns a
  corresponding File"
  ([to-file]
   (let [url (java.net.URL. manifest-url)
         out-file (io/as-file to-file)]
     (with-open [zip (io/input-stream url)]
       (io/copy zip out-file))
     out-file))
  ([]
   (download-zip "resources/MBTA_GTFS.zip")))

(defn- hyphenate [s]
  (str/replace s #"[_.]" "-"))

(defn- csv-vals [line]
  (map second (re-seq #"\"([^\"]+)\"" line)))

(defn- line->keys [line]
  (->> line
       (csv-vals)
       (map (comp keyword hyphenate))))

(defn get-feed-info []
  (let [body (line-seq (io/reader (java.net.URL. feed-info-url)))
        ks (line->keys (first body))
        vals (csv-vals (second body))]
    (into {} (map vector ks vals))))

(defn zip-reader
  [zip-path resource]
  (let [zip (java.util.zip.ZipFile. zip-path)]
    (-> (.getInputStream zip (.getEntry zip resource))
        (io/reader))))

(defn- vec->map [fields]
  (fn [vals]
    (into {} (map vector fields vals))))

(defn csv-to-maps [csv]
  (let [fields (map (comp keyword hyphenate) (first csv))]
    (map (vec->map fields) (rest csv))))

(defn get-csv [gtfs-path file-name]
  (-> (zip-reader gtfs-path file-name)
      (read-csv)
      (csv-to-maps)))

(defn get-trips []
  (get-csv gtfs-path "trips.txt"))

(defn get-stop-times []
  (get-csv gtfs-path "stop_times.txt"))

(defn get-stops []
  (get-csv gtfs-path "stops.txt"))

(defn get-shapes []
  (get-csv gtfs-path "shapes.txt"))
