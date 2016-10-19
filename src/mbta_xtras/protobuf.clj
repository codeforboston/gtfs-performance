(ns mbta-xtras.protobuf
  (:require [clojure.core.async :refer [<! >! alt! chan go-loop timeout] :as async]
            [clojure.java.io :as io]
            [mbta-xtras.db :as db])

  (:import clojure.lang.Reflector
           com.google.protobuf.CodedInputStream
           [com.google.transit.realtime GtfsRealtime GtfsRealtime$FeedMessage]))


(def trip-updates-url
  "http://developer.mbta.com/lib/GTRTFS/Alerts/TripUpdates.pb")


(defn get-feed [url]
  (GtfsRealtime$FeedMessage/parseFrom (io/input-stream url)))


(defn get-trip-updates []
  (get-feed trip-updates-url))


#_
(defn get-trip-updates []
  (let [updates (get-feed trip-updates-url)]
    (println (count (.getEntityList updates)))
    updates))


(defn stop-update->map [u]
  {:arrival-time (.. u getArrival getTime)
   :departure-time (.. u getDeparture getTime)
   :stop-id (.getStopId u)
   :stop-sequence (.getStopSequence u)})

(defn timestamp [u]
  (.. u getHeader getTimestamp))

(defn stop-updates-before [trip-update stamp]
  (take-while #(< (.getTime (.getArrival  %)) stamp)
              (.getStopTimeUpdateList trip-update)))

(defn trip-updates [^GtfsRealtime$FeedMessage message]
  (keep (memfn getTripUpdate) (. message getEntityList)))

(defn all-stop-updates
  "Processes an ISeq of trip updates into a lazy seq of maps containing information about all the stops "
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
  loop."
  [to & {:keys [interval close?]
         :or {interval 15000}}]
  (let [stop (chan)]
    (go-loop [last-stamp 0]
      (let [updates (get-trip-updates)
            stamp (.. updates getHeader getTimestamp)]
        (when (> stamp last-stamp)
          (doseq [update (all-stop-updates
                          (filter #(> (.getTimestamp %) last-stamp)
                                  (trip-updates updates)))]
            (>! to update)))

        (alt!
          (timeout interval) (recur stamp)
          stop nil)))))


