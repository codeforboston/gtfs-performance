(ns mbta-xtras.protobuf
  (:require [clojure.core.async :refer [>! chan go]]
            [clojure.java.io :as io]
            [mbta-xtras.db :as db])
  (:import clojure.lang.Reflector
           com.google.protobuf.CodedInputStream
           com.google.transit.realtime.GtfsRealtime
           [java.time LocalDate LocalDateTime LocalTime Instant]
           [java.time.format DateTimeFormatter]
           java.time.temporal.ChronoUnit))

(def vehicle-positions-url
  "http://developer.mbta.com/lib/GTRTFS/Alerts/VehiclePositions.pb")

(def trip-updates-url
  "http://developer.mbta.com/lib/GTRTFS/Alerts/TripUpdates.pb")

(def alerts-url
  "http://developer.mbta.com/lib/GTRTFS/Alerts/Alerts.pb")


(defn get-feed [url]
  (GtfsRealtime$FeedMessage/parseFrom (io/input-stream url)))

(defn get-vehicle-updates []
  (get-feed vehicle-positions-url))

(defn get-trip-updates []
  (get-feed trip-updates-url))


(defn stop-update->map [u]
  {:arrival-time (.. u getArrival getTime)
   :departure-time (.. u getDeparture getTime)
   :stop-id (.getStopId u)
   :stop-sequence (.getStopSequence u)})

(defn process-trip-update [u]
  {:delay (.getDelay u)
   :timestamp (.getTimestamp u)
   :stop-time-updates (mapv process-stop-time-update (.getStopTimeUpdateList u))
   :vehicle-id (.. u getVehicle getId)
   ;:route-id (. update getRouteId)
   })

(defn timestamp [u]
  (.. u getHeader getTimestamp))

(defn stop-updates-before [trip-update stamp]
  (take-while #(< (.getTime (.getArrival  %)) stamp)
              (.getStopTimeUpdateList trip-update)))

(defn trip-updates [^GtfsRealtime$FeedMessage message]
  (keep (memfn getTripUpdate) (. message getEntityList)))

(defn stuff [u]
  (mapcat (fn [tu]
            (let [start-date (.. tu getTrip getStartDate)
                  trip-id (.. tu getTrip getTripId)]
              (map (fn [stop-update]
                     {:stop-id (.getStopId stop-update)
                      :stop-sequence (.getStopSequence stop-update)
                      :arrival-time (.. stop-update getArrival getTime)
                      :trip-id trip-id
                      :trip-start start-date})
                   (.getStopTimeUpdateList tu))))
          (trip-updates u)))

(defn past-arrival-times
  "Returns pairs of [TripUpdate (past-arrivals)], where past arrivals are
  arrivals with a timestamp earlier than the timestamp of the message."
  [^GtfsRealtime$FeedMessage u]
  (let [stamp (timestamp u)]
    (map (fn [tu]
           [tu (map stop-update->map (stop-updates-before tu stamp))])
         (trip-updates u))))

(defn datetime-for-stamp [stamp]
  (LocalDateTime/from (Instant/ofEpochSecond stamp)))

(def datetime-format (DateTimeFormatter/ofPattern "yyyyMMdd"))
(defn datetime-for-str [date-str]
  (LocalDateTime/of (LocalDate/parse date-str datetime-format)
                    LocalTime/MIDNIGHT))

(defn offset-time
  "Returns a new LocalDateTime that is offset from the reference date by the
  number of hours, minutes, and seconds given in the time-str."
  [ref-date time-str]
  (let [[h m s] (re-find #"(\d\d?):(\d\d):(\d\d)" time-str)]
    (-> ref-date
        (.truncatedTo ChronoUnit/DAYS)
        (.plusHours (long h))
        (.plusMinute (long m))
        (.plusSeconds (long s)))))

(defn late-arrivals [db arrivals]
  (keep (fn [arrival]
          (let []))))

;; Turns out not to be useful for the MBTA feed!
(defn get-delayed-stops
  "Takes the StopTimeUpdates for a given TripUpdate and returns only those
  updates that represent a late arrival time."
  [trip-update]
  (filter #(pos? (.. % getArrival getDelay))
          (.getStopTimeUpdateList trip-update)))

(defn get-delayed-trip-stops
  "Takes an iterable of FeedEntity objects and filters out those that have at
  least one stop update reported as delayed."
  [updates]
  (keep (fn [update]
         (let [trip-update (.getTripUpdate update)
               trip (.getTrip trip-update)
               delayed (get-delayed-stops trip-update)]
           (when (seq delayed)
             {:trip-id (.getTripId trip)
              :trip-start (.getStartDate trip)
              :route-id (.getRouteId trip)
              :delays (map stop-update->map delayed)})))
        updates))


(defn find-delays
  ([u stamp]
   ((.getEntityList u)))
  ([u]
   (find-delays u (timestamp u))))

;; Notes: It seems like the MBTA's TripUpdates feed does not actually report
;; delays.
