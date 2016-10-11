(ns mbta-xtras.mongo
  (:require [com.stuartsierra.component :as component]
            [monger.core :as mg]
            [monger.core :as mg]

            [environ.core :refer [env]]
            [monger.collection :as mc]))

(defn setup-db [db]
  (try
    (mc/ensure-index db "stops" (array-map :stop-name "text"))
    (mc/ensure-index db "stops" (array-map :stop-name 1))
    (mc/ensure-index db "stops" (array-map :coords "2dsphere"))

    (mc/ensure-index db "shapes" (array-map :path "2dsphere"))
    (catch Exception _exc)))

(defrecord MongoDB [uri]
  component/Lifecycle
  (start [this]
    (cond-> this
      (not (:mongo this))
      (merge this
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
