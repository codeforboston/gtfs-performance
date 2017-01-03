(ns mbta-xtras.protobuf
  (:require [clojure.core.async :refer [<! >! alt! chan go-loop timeout] :as async]
            [clojure.java.io :as io]
            [clojure.spec :as s]
            [environ.core :refer [env]]
            [taoensso.timbre :refer [log info error]]
            [clojure.string :as str])

  (:import [com.google.transit.realtime GtfsRealtime$FeedMessage
            GtfsRealtime$TripUpdate$StopTimeUpdate]
           [com.google.protobuf InvalidProtocolBufferException]))


(def trip-updates-url
  (env :trip-updates-url "http://developer.mbta.com/lib/GTRTFS/Alerts/TripUpdates.pb"))

(defn ^GtfsRealtime$FeedMessage get-feed [url]
  (GtfsRealtime$FeedMessage/parseFrom (io/input-stream url)))

(defn get-trip-updates
  "Retrieve the latest TripUpdates from the specified URL, or from the default.
  Note that since this is designed with the MBTA's GTFS-RT feed in mind, the
  update types (trip, vehicle, alert) are delivered on separate feed."
  [& [url]]
  (get-feed (or url trip-updates-url)))

(defn timestamp [u]
  (.. u getHeader getTimestamp))

(defn stop-updates-before [trip-update stamp]
  (take-while #(< (.getTime (.getArrival  %)) stamp)
              (.getStopTimeUpdateList trip-update)))

(defn trip-updates [^GtfsRealtime$FeedMessage message]
  (keep (memfn getTripUpdate) (. message getEntityList)))

(defn schedule-relationship [^GtfsRealtime$TripUpdate$StopTimeUpdate update]
  (if (.hasScheduleRelationship update)
    (keyword (str/lower-case (str (.getScheduleRelationship update))))

    :scheduled))

(defn all-stop-updates
  "Processes an ISeq of trip updates into a lazy seq of maps containing
  information about all the stop updates reported on each trip. Maps have an :id
  attribute that uniquely identifies the trip's arrivals.

  Returns a transducer if no argument is provided."
  ([trip-updates]
   (sequence (all-stop-updates) trip-updates))
  ([]
   (mapcat (fn [tu]
             (let [start-date (.. tu getTrip getStartDate)
                   trip-id (.. tu getTrip getTripId)
                   route-id (.. tu getTrip getRouteId)]
               (map (fn [stop-update]
                      (let [rel (schedule-relationship stop-update)]
                        (merge
                         {:stop-id (. stop-update getStopId)
                          :stop-sequence (. stop-update getStopSequence)
                          :trip-id trip-id
                          :route-id route-id
                          :trip-start start-date
                          ;; Unique ID.  We may get multiple updates for the same
                          ;; stop on the same trip. We'll assume that later updates
                          ;; are going to better represent the true arrival time of
                          ;; the vehicle.
                          ;; Since this is really a storage consideration, maybe
                          ;; move this elsewhere? If the storage backend supported
                          ;; it, we'd be using a compound index.
                          :id (str trip-id "-" start-date "-" (.getStopId stop-update))}

                         (if (= rel :scheduled)
                           {:arrival-time (long (.. stop-update getArrival getTime))
                            :departure-time (long (.. stop-update getDeparture getTime))}

                           {:schedule-relationship (str rel)}))))
                    (.getStopTimeUpdateList tu)))))))

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
                            ;; due to transient network issues, quotas, or (I
                            ;; suspect) non-atomic writing to the protobuf on
                            ;; the part of the MBTA.
                            (catch InvalidProtocolBufferException ipb-ex
                              ;; Don't print the whole stacktrace and unfinished
                              ;; message, since it's really spammy.
                              (error (.getMessage ipb-ex)))
                            (catch Exception ex
                              (error ex "encountered while fetching trip updates")))]
        (let [stamp (.. updates getHeader getTimestamp)]
          (when (> stamp last-stamp)
            (doseq [update (->> (trip-updates updates)
                                (filter #(> (.getTimestamp %) last-stamp))
                                (all-stop-updates))]
              (>! to (assoc update :stamp stamp))))

          (alt!
            (timeout interval) (recur stamp)
            stop nil))

        (recur last-stamp)))
    stop))
