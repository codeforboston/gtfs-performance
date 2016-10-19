(ns mbta-xtras.protobuf
  (:require [clojure.core.async :refer [<! >! alt! chan go-loop timeout] :as async]
            [clojure.java.io :as io]
            [environ.core :refer [env]]
            [mbta-xtras.db :as db])

  (:import [com.google.transit.realtime GtfsRealtime$FeedMessage]))


(def trip-updates-url
  (env :trip-updates-url "http://developer.mbta.com/lib/GTRTFS/Alerts/TripUpdates.pb"))


(defn ^GtfsRealtime$FeedMessage get-feed [url]
  (GtfsRealtime$FeedMessage/parseFrom (io/input-stream url)))


(defn get-trip-updates
  "Retrieve the latest TripUpdates from the specified URL, or from the default."
  [& [url]]
  (get-feed (or url trip-updates-url)))


(defn get-trip-updates
  "Note that since this is designed with the MBTA's GTFS-RT feed in mind, the
  update types (trip, vehicle, alert) are "
  []
  (let [updates (get-feed trip-updates-url)]
    (println (count (.getEntityList updates)))
    updates))


(defn timestamp [u]
  (.. u getHeader getTimestamp))

(defn stop-updates-before [trip-update stamp]
  (take-while #(< (.getTime (.getArrival  %)) stamp)
              (.getStopTimeUpdateList trip-update)))

(defn trip-updates [^GtfsRealtime$FeedMessage message]
  (keep (memfn getTripUpdate) (. message getEntityList)))

(defn all-stop-updates
  "Processes an ISeq of trip updates into a lazy seq of maps containing
  information about all the stop updates reported on each trip. Maps have an :id
  attribute that uniquely identifies the trip's arrivals"
  [trip-updates]
  (mapcat (fn [tu]
            (let [start-date (.. tu getTrip getStartDate)
                  trip-id (.. tu getTrip getTripId)]
              (map (fn [stop-update]
                     {:stop-id (.getStopId stop-update)
                      :stop-sequence (.getStopSequence stop-update)
                      :arrival-time (.. stop-update getArrival getTime)
                      :trip-id trip-id
                      :trip-start start-date
                      ; Unique ID.  We may get multiple updates for the same
                      ; stop on the same trip. We'll assume that later updates
                      ; are going to better represent the true arrival time of
                      ; the vehicle.
                      :id (str trip-id "-" start-date "-" (.getStopId stop-update))})
                   (.getStopTimeUpdateList tu))))
          trip-updates))

(defn trip-updates->!
  "Retrieve trip stop updates at an interval, process them, and put them on a
  channel. Returns a channel that, when closed or given a value, will stop the
  loop, possibly after a short delay."
  [to & {:keys [interval close?]
         :or {interval 30000}}]
  (let [stop (chan)]
    (go-loop [last-stamp 0]
      (if-let [updates (try (get-trip-updates)
                            ;; Ignore errors; we'll assume for now that they're
                            ;; due to transient network issues or quota limits.
                            (catch Exception ex (prn ex)))]
        (let [stamp (.. updates getHeader getTimestamp)]
          (when (> stamp last-stamp)
            (doseq [update (->> (trip-updates updates)
                                (filter #(> (.getTimestamp %) last-stamp))
                                (all-stop-updates))]
              (>! to update)))

          (alt!
            (timeout interval) (recur stamp)
            stop nil))

        (recur last-stamp)))
    stop))


