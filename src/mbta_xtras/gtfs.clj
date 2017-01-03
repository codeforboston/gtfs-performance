(ns mbta-xtras.gtfs
  "Some utilities for reading a zipped GTFS manifest."
  (:require [aleph.http :as http]
            [environ.core :refer [env]]
            [clojure.core.async :refer [>! <! chan go-loop timeout]]
            [clojure.data.csv :refer [read-csv]]
            [clojure.java.io :as io]
            [clojure.string :as str]

            [mbta-xtras.utils :as $]
            [clojure.core.async :as async])
  (:import [java.time Instant ZonedDateTime]
           [java.time.format DateTimeFormatter]
           [java.util.zip ZipInputStream]))


(def gtfs-path
  (env :manifest-download-to "resources/MBTA_GTFS.zip"))

(def feed-info-url
  (env :feed-info-url
       "http://www.mbta.com/uploadedfiles/feed_info.txt"))

(def manifest-url
  "The URL for the zip file containing the full trip information."
  (env :manifest-url
       "http://www.mbta.com/uploadedfiles/MBTA_GTFS.zip"))

(defn http-date-time [s]
  (.parse DateTimeFormatter/RFC_1123_DATE_TIME s))

(defn last-modified [resp]
  (some-> resp
          (get-in [:headers "last-modified"])
          (http-date-time)
          (Instant/from)
          (.getEpochSecond)))

(defn download-zip
  "Saves the GTFS file specified in the environment to file and returns a
  corresponding java.util.File."
  ([to-file]
   (let [url (java.net.URL. manifest-url)
         out-file (io/as-file to-file)]
     (with-open [zip (io/input-stream url)]
       (io/copy zip out-file))
     out-file))
  ([]
   (download-zip gtfs-path)))

(defn- hyphenate [s]
  (str/replace s #"[_.]" "-"))

(defn zip-reader
  [zip-path resource]
  (let [zip (java.util.zip.ZipFile. zip-path)]
    (-> (.getInputStream zip (.getEntry zip resource))
        (io/reader))))

(defn zip-input-stream []
  (java.util.zip.ZipInputStream. (io/input-stream (java.net.URL. manifest-url))))

(defn zip-entries [^ZipInputStream zi]
  (take-while (complement nil?) (repeatedly #(.getNextEntry zi))))

(defn zip-seek
  "Positions the ZipInputStream at the start of the entry with the given file
  name, if it exists. If there is no such entry, returns nil."
  [^ZipInputStream zi file-name]
  (first (filter #(= (.getName %) file-name) (zip-entries zi))))

(defn- vec->map [fields]
  (fn [vals]
    (into {} (map vector fields vals))))

(defn reader-maps [csv-rdr]
  (let [[header & rows] (read-csv csv-rdr)]
    (let [fields (map (comp keyword hyphenate) header)]
            (map (vec->map fields) rows))))

(defn latest-feed-info
  "Retrieves the latest feed info."
  []
  (let [resp @(http/get (or feed-info-url manifest-url))
        input (if feed-info-url
                (:body resp)

                (let [zip-in (ZipInputStream. (:body resp))
                      entry (zip-seek zip-in "feed_info.txt")]
                  zip-in))]
    (-> (io/reader input)
        (reader-maps)
        (first)
        (assoc :modified (last-modified resp)))))

(defn feed-updates
  "Starts a go loop that periodically checks for the latest information about
  the GTFS feed. Returns a channel that receives a info map each time a change
  is detected."
  [& [interval]]
  (let [out-chan (chan)]
    (go-loop [info nil]
      (let [to (timeout (or interval 86400000))]
        (if (or (not info)
                (> (last-modified @(http/head (or feed-info-url
                                                  manifest-url)))
                   (:modified info)))
          (let [new-info (latest-feed-info)]
            ;; Exit if the out-chan is closed, otherwise, repeat loop.
            (when (or (= new-info info)
                      (>! out-chan new-info))
              (<! to)
              (recur new-info)))

          (do
            (<! to)
            (recur info)))))
    out-chan))

(defn iterate-gtfs-files
  "Returns a lazy sequence of [file_name, map-seq] from the latest GTFS
  manifest."
  []
  (let [resp @(http/get manifest-url)
        input (ZipInputStream. (:body resp))]
    (map (fn [entry] [(.getName entry) (io/reader input)])
         (zip-entries input))))
