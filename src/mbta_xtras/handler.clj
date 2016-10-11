(ns mbta-xtras.handler
  (:require [compojure.core :refer [GET POST defroutes]]))


(defn handler [req]
  {:status 200
   :headers {"content-type" "text/plain"}
   :body (prn-str req)})

(defn make-app [conn]
  (fn [req]
    (#'handler (assoc req :conn conn))))
