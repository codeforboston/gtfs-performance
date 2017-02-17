(ns mbta-xtras.analysis
  (:require [clojure.core.async :as async :refer [alt! go-loop <!!]]

            [monger.collection :as mc]

            [mbta-xtras.db :as db]
            [mbta-xtras.protobuf :refer [vehicle-updates->]]
            [mbta-xtras.trip-performance :refer [travel-times]]
            [mbta-xtras.system :refer [system]]))


(defn collect-trips
  "Tap the trip update recorder channel for `wait` seconds and collect the trip
  ids for the updates observed in that time. Returns a promise that is delivered
  after the time is up."
  [wait]
  (if-let [mult (-> system :recorder :mult)]
    (let [in (async/tap mult (async/chan))
          done (async/timeout wait)
          result (promise)]
      (go-loop [changed #{}]
        (alt! done (do
                     (async/untap mult in)
                     (async/close! in)
                     (deliver result changed))
              in ([update]
                  (recur (conj changed [(:trip-id update) (:trip-start update)])))))
      result)

    (throw (RuntimeException. "The system is not started"))))

(defn get-one-update []
  (let [mult (-> system :recorder :mult)
        in (async/tap mult (async/chan))
        update (async/<!! in)]
    (async/close! in)
    update))

(def trip-updates nil)

(defn tap-updates
  "Tap the trip updates mult and store the new channel in the trip-updates var.
  `n` specifies the number of recent updates to store in the buffer."
  [& [n]]
  (alter-var-root #'trip-updates
                  (fn [x]
                    (or x (async/tap (get-in system [:recorder
                                                     :mult])
                                     (async/chan
                                      (async/sliding-buffer (or n 50))))))))

(defn untap-updates []
  (alter-var-root #'trip-updates (fn [x]
                                   (when x
                                     (async/close! x)
                                     (async/untap (get-in system [:recorder
                                                                  :mult])
                                                  x))
                                   nil)))

(def vehicle-updates nil)

(defn tap-vehicles []
  (alter-var-root #'vehicle-updates
                  (fn [x]
                    (or x (let [c (async/chan (async/sliding-buffer 100))]
                            (vehicle-updates-> c)
                            c)))))

(defn untap-vehicles []
  (alter-var-root #'vehicle-updates
                  (fn [c]
                    (when c
                      (async/close! c)
                      nil))))

; (travel-times (-> system :mongo :db) "70067" "70075" 1483562006 1483565521)

(defn get-db [] (-> system :mongo :db))

#_
(:trip-id (first (db/trips-for-route (get-db) "1")))

#_
(db/stop-times-for-trip (get-db) "32795733")

#_
("110" "2168" "2166" "2167" "66" "67" "68" "69" "71" "72" "73" "74" "75" "77" "79" "80" "82" "187" "83" "84" "59" "854" "856" "10100" "10101" "62" "63" "64")
#_
(db/stop-times-for-trip (-> system :mongo :db) )
#_
(def from-stops (mc/find-maps (get-db) "processed-trip-stops" {:stop-id "110"
                                                               :arrival-time {:$gte 1487277570
                                                                              :$lte 1487278492}}))
#_
(into #{} (map :trip-id) from-stops)
; 1483562006&to-datetime=1483565521&from-stop=70067&to-stop=70075


