(ns mbta-xtras.web
  (:require [aleph.http :as http]
            [com.stuartsierra.component :as component]
            [environ.core :refer [env]]
            [mbta-xtras.handler :refer [make-app]]))

(defrecord WebServer [port]
  component/Lifecycle
  (start [this]
    (assoc this
           :webserver (http/start-server
                       (make-app (-> this :mongo :conn)
                                 (-> this :mongo :db))
                       {:port port})))

  (stop [this]
    (when-let [server (:webserver this)]
      (.close server))
    (dissoc this :webserver)))

(defn make-server []
  (->WebServer (env :http-port 3141)))
