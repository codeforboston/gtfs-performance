(ns mbta-xtras.gtfs
  "Some utilities for reading a zipped GTFS manifest."
  (:require [aleph.http :as http]
            [environ.core :refer [env]]
            [clojure.core.async :refer [>! <! chan go-loop timeout]]
            [clojure.data.csv :refer [read-csv]]
            [clojure.java.io :as io]
            [clojure.string :as str]

            [mbta-xtras.utils :as $])
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

(defonce agencies (atom {}))
(defonce feed-info (atom nil))
(defonce calendar (atom nil))

(defn last-modified [resp]
  (some-> resp
          (get-in [:headers "last-modified"])
          (http-date-time)
          (Instant/from)))

(defn info-last-modified []
  (last-modified @(aleph.http/head (or feed-info-url manifest-url))))

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

(defn csv-to-maps [[header & rows]]
  (let [fields (map (comp keyword hyphenate) header)]
    (map (vec->map fields) rows)))

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
        (read-csv)
        (csv-to-maps)
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
                (pos? (.compareTo (last-modified @(http/head (or feed-info-url
                                                                 manifest-url)))
                                  (:modified info))))
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

(defn iterate-csv []
  (let [resp @(http/get manifest-url)
        input (ZipInputStream. (:body resp))]
    (map (fn [entry] [(.getName entry) (csv-to-maps (read-csv (io/reader
                                                               input)))])
         (zip-entries input))))

(defn get-csv [gtfs-path file-name]
  (when-not (.exists (io/file gtfs-path))
    (download-zip gtfs-path))
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

(defn get-agencies []
  (into {} (map (juxt :agency-id identity))
        (get-csv gtfs-path "agency.txt")))

(defn get-calendar []
  (into {} (map (juxt :service-id identity))
        (get-csv gtfs-path "calendar.txt")))

(defn agency-info [id]
  (get (or @agencies (swap! agencies merge (get-agencies))) id))

(defn service-calendar [service-id]
  (get (or @calendar
           (reset! calendar (get-calendar)))
       service-id))
