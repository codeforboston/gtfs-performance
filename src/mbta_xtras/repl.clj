(ns mbta-xtras.repl
  (:require [mbta-xtras.db :as db]
            [mbta-xtras.gtfs :as gtfs]
            [mbta-xtras.mongo :refer [make-mongo]]
            [mbta-xtras.trip-performance :as trips]
            [mbta-xtras.system :as system]

            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]
            [taoensso.timbre.appenders.core :refer [println-appender]]

            ;; These are here for interactive exploration
            [mbta-xtras.analysis :refer [collect-trips]]
            [clojure.core.async :refer [<! alt! go-loop] :as async]))


(def mongo
  "Contains a record representing the MongoDB component. When a connection is
  open, its `:conn` key holds a MongoClient reference and its `:db` key holds a
  DB reference."
  (make-mongo))

(def db nil)

(defn start-mongo []
  (alter-var-root #'db
                  (constantly (:db (alter-var-root #'mongo component/start)))))

(defn stop-mongo
  "Convenience function that closes the connection to MongoDB and resets the
  `mongo` and `db` variables."
  []
  (alter-var-root #'db (constantly nil))
  (alter-var-root #'mongo component/stop))

(defn start-all []
  ;; (timbre/merge-config!
  ;;  {:appenders {:println (assoc (println-appender {:stream :auto})
  ;;                               :min-level :warn)}})
  (system/start))

(defn stop-all []
  (system/stop))
