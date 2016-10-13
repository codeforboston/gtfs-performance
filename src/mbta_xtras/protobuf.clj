(ns mbta-xtras.protobuf
  (:require [clojure.java.io :as io]
            [clojure.core.async :refer [>! chan go]])
  (:import [com.google.transit.realtime GtfsRealtime$FeedMessage]
           [com.google.protobuf CodedInputStream]
           [clojure.lang Reflector]))

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


(defn process-stop-time-update [u]
  {:arrival-time (.. u getArrival getTime)
   :departure-time (.. u getDeparture getTime)
   :stop-id (.getStopId u)
   :stop-sequence (.getStopSequence u)})

(defn process-trip-update [update]
  {:delay (.getDelay update)
   :timestamp (.getTimestamp update)
   :stop-time-updates (mapv process-stop-time-update (.getStopTimeUpdateList update))
   :vehicle-id (.. update getVehicle getId)
   ;:route-id (. update getRouteId)
   })

