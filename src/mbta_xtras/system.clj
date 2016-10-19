(ns mbta-xtras.system
  (:require [com.stuartsierra.component :as component]
            [mbta-xtras
             [mongo :as mongo]
             [trip-performance :refer [make-recorder]]
             [web :as web]])
  (:gen-class))


(defn api-system
  "Returns a new system map describing the default components of the application
  and their interdependencies. Run `component/start-system` on the result to
  start running the application."
  []
  (component/system-map
   :mongo (mongo/make-mongo)
   :recorder (component/using
              (make-recorder)
              {:mongo :mongo})
   :webserver (component/using
               (web/make-server)
               {:mongo :mongo})))

(def system (api-system))

(defn start []
  (alter-var-root #'system component/start-system))

(defn stop []
  (alter-var-root #'system component/stop-system))

(defn -main [] (start))
