(ns mbta-xtras.repl
  (:require [mbta-xtras.db :as db]
            [mbta-xtras.gtfs :as gtfs]
            [mbta-xtras.mongo :refer [make-mongo]]
            [mbta-xtras.trip-performance :as trips]
            [mbta-xtras.system :as system]

            [com.stuartsierra.component :as component]))


(def mongo (make-mongo))

(def db nil)

(defn start-mongo []
  (alter-var-root #'db
                  (constantly (:db (alter-var-root #'mongo component/start)))))

(defn stop-mongo []
  (alter-var-root #'db (constantly nil))
  (alter-var-root #'mongo component/stop))
