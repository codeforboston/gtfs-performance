(ns mbta-xtras.protobuf
  (:require [clojure.core.async :refer [<! >! chan go-loop timeout] :as async]
            [clojure.java.io :as io]
            [mbta-xtras.db :as db])

  (:import clojure.lang.Reflector
           com.google.protobuf.CodedInputStream
           [com.google.transit.realtime GtfsRealtime GtfsRealtime$FeedMessage]
           [java.time LocalDate LocalDateTime LocalTime Instant]
           [java.time.format DateTimeFormatter]
           java.time.temporal.ChronoUnit))


(def trip-updates-url
  "http://developer.mbta.com/lib/GTRTFS/Alerts/TripUpdates.pb")


(defn get-feed [url]
  (GtfsRealtime$FeedMessage/parseFrom (io/input-stream url)))


(defn get-trip-updates []
  (get-feed trip-updates-url))


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
                      :trip-stop-id (str trip-id "-" start-date "-" (.getStopId stop-update))})
                   (.getStopTimeUpdateList tu))))
          trip-updates))


(defn pipe-trip-updates
  "Retrieve trip stop updates at an interval, process them, and put them on a
  channel. Returns a go channel."
  [to & {:keys [interval close?]
         :or {interval 15000
              close? true}}]
  (go-loop [last-stamp 0]
    (let [updates (get-trip-updates)
          stamp (.. updates getHeader getTimestamp)]

      ;; We'll continue running if the stamp indicates that the update is old OR
      ;; if the update is new and we succeed in putting all the updates on the
      ;; channel. This allows the user to halt the loop by closing the `to`
      ;; channel.
      (when (or (<= stamp last-stamp)
                (not-any? nil?
                          (for [update (all-stop-updates (filter #(> (.getTimestamp %) last-stamp)
                                                                 (trip-updates updates)))]
                            (>! to update)))
                (not close?))
        (<! (timeout interval))
        (recur stamp)))))


(defn trip-updates-chan
  "Creates and returns a new channel that will receive trip stop update maps as
  they become available. Older values will be dropped if not consumed."
  []
  (let [c (chan (async/sliding-buffer 100))]
    (pipe-trip-updates c)
    c))
