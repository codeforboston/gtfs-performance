(ns mbta-xtras.system
  (:require [com.stuartsierra.component :as component]
            [mbta-xtras
             [mongo :as mongo]
             [trip-performance :refer [make-recorder]]
             [web :as web]]
            [taoensso.timbre :as timbre]
            [taoensso.timbre.appenders.3rd-party.rotor :refer [rotor-appender]])
  (:gen-class))

;; Standard logging configuration.
(timbre/merge-config!
 {:appenders {:rotor (assoc (rotor-appender
                             {:path "logs/log.log"
                              :max-size (* 10 1024 1024)})
                            :output-fn (partial timbre/default-output-fn
                                                {:stacktrace-fonts {}}))
              :println nil}})

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
