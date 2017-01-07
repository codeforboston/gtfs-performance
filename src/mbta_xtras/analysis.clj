(ns mbta-xtras.analysis
  (:require [clojure.core.async :as async :refer [alt! go-loop <!!]]

            [monger.collection :as mc]

            [mbta-xtras.db :as db]
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

; (travel-times (-> system :mongo :db) "70067" "70075" 1483562006 1483565521)

; 1483562006&to-datetime=1483565521&from-stop=70067&to-stop=70075

; ("33113908" "20170104")

