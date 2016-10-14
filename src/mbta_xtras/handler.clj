(ns mbta-xtras.handler
  (:require [clojure.data.json :as json]
            [compojure.core :refer [context defroutes GET POST]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.params :refer [wrap-params]]
            [mbta-xtras.db :as db]
            [clojure.string :as str]))

(defn find-stops [{:keys [mongo params]}]
  {:status 200
   :headers {"Content-type" "application/json"}
   :body (json/write-str (db/find-stops mongo params))})

#_
(defn )

;; (defroutes handler
;;   (context "/xapi" []
;;     (GET "/find_stops" find-stops)))

(def handler
  find-stops)

(def app
  (-> #'handler
      (wrap-keyword-params)
      (wrap-params)))

(defn make-app [conn]
  (fn [req]
    (app (assoc req :mongo conn))))
