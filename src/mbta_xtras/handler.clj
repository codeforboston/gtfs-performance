(ns mbta-xtras.handler
  (:require [clojure.data.json :as json]
            [compojure.core :refer [context defroutes GET POST]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.params :refer [wrap-params]]
            [mbta-xtras.api-spec :as api]
            [mbta-xtras.db :as db]
            [mbta-xtras.trip-performance :as trip]
            [clojure.spec :as s]
            [clojure.string :as str]))


(defn explain-problem [{:keys [path pred]}]
  (str (when path
         (str (str/join "." (map name path)) " "))

       (case (first pred)
         contains? (str "missing parameter: " (name (last pred)))
         (re-matches re-find) (str "doesn't match pattern: "
                                   (second pred))
         (str "doesn't match predicate: " pred))))

(defn error-message [expl]
  (map explain-problem (::s/problems expl)))

(defn find-stops [{:keys [mongo params]}]
  {:status 200
   :headers {"Content-type" "application/json"}
   :body (json/write-str (db/find-stops mongo params))})

(defn trip-performance [{:keys [db params]}]
  (if-let [expl (s/explain-data ::api/trip-performance-request params)]
    {:status 400
     :headers {"Content-type" "application/json"}
     :body (json/write-str
            {:errors (error-message expl)
             :request-params params})}

    {:status 200
     :headers {"Content-type" "application/json"}
     :body (json/write-str
            (let [results (trip/trip-performance db
                                                 (:trip-id params)
                                                 (:trip-start params))]
              {:performance results}))}))

(defroutes handler
  (context "/xapi" []
           (GET "/find_stops" []  find-stops)
           (GET "/trip_performance" []  trip-performance)))

(def app
  (-> #'handler
      (wrap-keyword-params)
      (wrap-params)))

(defn make-app [conn db]
  (fn [req]
    (app (assoc req :mongo conn, :db db))))
