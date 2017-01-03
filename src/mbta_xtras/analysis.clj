(ns mbta-xtras.analysis
  (:require [clojure.core.async :as async :refer [alt! go-loop]]

            [mbta-xtras.db :as db]
            [mbta-xtras.system :refer [system]]))

(defn collect-trips
  [wait]
  (if-let [mult (-> system :recorder :mult)]
    (let [in (async/tap mult (async/chan))
          done (async/timeout wait)
          result (promise)]
      (go-loop [changed #{}]
        (alt! done (deliver result changed)
              in ([update]
                  (recur (conj changed [(:trip-id update) (:trip-start update)])))))
      result)

    (throw (RuntimeException. "The system is not started"))))


