(ns mbta-xtras.nrepl
  (:require [clojure.tools.nrepl.server :as nrepl]
            [com.stuartsierra.component :as component]))


(defrecord NReplServer [port]
  component/Lifecycle
  (start [this]
    (assoc this :nrepl-server (nrepl/start-server :port port)))

  (stop [this]
    (when-let [server (:nrepl-server this)]
      (nrepl/stop-server server))
    (dissoc this :nrepl-server)))

(defn make-server [port] (->NReplServer port))
