(ns mbta-xtras.scratch
  (:require [clojure.core.async :refer [>! chan go-loop timeout]]
            [clojure.java.io :as io]
            [mbta-xtras.db :as db]
            [monger.collection :as mc])

  (:import clojure.lang.Reflector
           com.google.protobuf.CodedInputStream
           com.google.transit.realtime.GtfsRealtime
           [java.time LocalDate LocalDateTime LocalTime Instant]
           [java.time.format DateTimeFormatter]
           java.time.temporal.ChronoUnit))

(def vehicle-positions-url
  "http://developer.mbta.com/lib/GTRTFS/Alerts/VehiclePositions.pb")

(def alerts-url
  "http://developer.mbta.com/lib/GTRTFS/Alerts/Alerts.pb")


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

(defn past-arrival-times
  "Returns pairs of [TripUpdate (past-arrivals)], where past arrivals are
  arrivals with a timestamp earlier than the timestamp of the message."
  [^GtfsRealtime$FeedMessage u]
  (let [stamp (timestamp u)]
    (map (fn [tu]
           [tu (map stop-update->map (stop-updates-before tu stamp))])
         (trip-updates u))))


;; Turns out not to be useful for the MBTA feed, since it doesn't report delays!
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

(defn trip-performance
  "Take the observed trip stop updates and the scheduled stop arrival times and
  use them to calculate how far ahead or behind schedule each stop of the trip
  was. When there is no information about a trip's arrival time at a stop,
  calculate it by taking the first known "
  [db trip-id start-date]
  (let [scheduled (stop-times db trip-id start-date)
        trip-stops (sort-by :stop-sequence
                            (mc/find-maps db "trip-stops" {:trip-id trip-id
                                                           :trip-start start-date}))]
    (->> (concat (mapcat (fn [[from-stop to-stop]]
                           (let [start (:stop-sequence from-stop)
                                 end (:stop-sequence to-stop)
                                 sched-start (:scheduled-arrival (scheduled start))
                                 total-sched (- (:scheduled-arrival (scheduled end)) sched-start)
                                 total-obs (- (:arrival-time to-stop)
                                              (:arrival-time from-stop))]
                             (map (fn [sched]
                                    (let [percent (/ (- (:arrival-time sched)
                                                        sched-start) total-sched)]
                                      [(:stop-sequence sched)
                                       {:arrival-time (* percent total-obs)
                                        :departure-time nil ;; use scheduled dwell time
                                        :estimated? true}]))
                                  (map #(get scheduled %) (range (inc start) end)))))
                         (partition 2 1 trip-stops))
                 (keep (partial add-stop-delay (comp :scheduled-arrival scheduled)) trip-stops))
         (sort-by :stop-sequence))))
