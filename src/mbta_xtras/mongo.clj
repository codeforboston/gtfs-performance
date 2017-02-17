(ns mbta-xtras.mongo
  (:require [com.stuartsierra.component :as component]
            [monger.core :as mg]

            [environ.core :refer [env]]
            [monger.collection :as mc]))

(defn setup-db [db]
  (mc/ensure-index db "stops" {:stop-name "text"})
  (mc/ensure-index db "stops" {:stop-name 1})
  (mc/ensure-index db "stops" {:coords "2dsphere"})
  (mc/ensure-index db "stops" {:stop-id 1} {:unique true})

  (mc/ensure-index db "shapes" {:path "2dsphere"})

  (mc/ensure-index db "trips" {:shape-id 1})
  (mc/ensure-index db "trips" {:trip-id 1})
  (mc/ensure-index db "trips" {:route-id 1})

  (mc/ensure-index db "stop-times" {:trip-id 1})
  (mc/ensure-index db "stop-times" {:stop-id 1})
  (mc/ensure-index db "stop-times" {:stop-sequence 1})

  (mc/ensure-index db "trip-stops" {:id 1} {:unique true})
  (mc/ensure-index db "trip-stops" (array-map :stop-id 1 :arrival-time 1))
  (mc/ensure-index db "trip-stops" {:stamp -1})
  (mc/ensure-index db "trip-stops" {:trip-id 1})
  (mc/ensure-index db "trip-stops" {:trip-start 1})

  (mc/ensure-index db "processed-trip-stops" {:stop-id 1})
  (mc/ensure-index db "processed-trip-stops" {:trip-id 1})
  (mc/ensure-index db "processed-trip-stops" {:arrival-time -1})

  (mc/ensure-index db "trip-stops" {:id 1} {:unique true})
  (mc/ensure-index db "processed-trip-stops" {:id 1} {:unique true}))


(defrecord MongoDB [uri]
  component/Lifecycle
  (start [this]
    (cond-> this
      (not (:mongo this)) (merge this
             (let [m (mg/connect-via-uri uri)]
               (setup-db (:db m))
               {:conn (:conn m)
                :db (:db m)}))))

  (stop [this]
    (when-let [conn (:conn this)]
      (mg/disconnect conn))
    (dissoc this :conn :db)))

(defn make-mongo []
  (->MongoDB (env :mongo-uri "mongodb://localhost/mbta")))
