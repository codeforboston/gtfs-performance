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


(defn explain-pred [pred]
  (case (first pred)
    contains? (str "missing parameter: " (name (last pred)))

    (or and) (str/join (str " " (str/upper-case (str (first pred))) " ")
                       (map #(str "(" (explain-pred %) ")") (rest pred)))

    (re-matches re-find) (str "doesn't match pattern: "
                              (second pred))
    (str "doesn't match predicate: " pred)))

(defn explain-problem [{:keys [path pred]}]
  (str (when path
         (str (str/join "." (map name path)) " "))

       (explain-pred pred)))

(defn error-message [expl]
  (map explain-problem (::s/problems expl)))

(defn api-endpoint [spec f]
  (fn [{:keys [params] :as req}]
    (if-let [expl (s/explain-data spec params)]
      {:status 400
       :headers {"Content-type" "application/json"}
       :body (json/write-str {:errors (error-message expl)
                              :request-params params})}

      (f req))))

(defn wrap-json [x]
  (if (map? x)
    x
    {:status 200
     :headers {"Content-type" "application/json"}
     :body x}))

(defmacro defapi [name spec params & body]
  `(def ~name (api-endpoint ~spec (fn ~params
                                    (wrap-json ~@body)))))

(defapi find-stops ::api/find-stops-query
  [{:keys [db params]}]
  (json/write-str (db/find-stops db params)))

(defapi trip-performance ::api/trip-performance-request
  [{:keys [db params]}]
  (json/write-str
   (let [results (trip/trip-performance db
                                        (:trip-id params)
                                        (:trip-start params))]
     {:performance results})))

(defapi travel-times ::api/travel-times-request
  [{:keys [db params]}]
  (let [{:keys [from-datetime to-datetime
                from-stop to-stop]} params]
    (trip/travel-times db from-stop to-stop
                       (Long/parseLong from-datetime)
                       (Long/parseLong to-datetime))))

(defapi trips-for-stop ::api/trips-for-stop-request
  [{:keys [db params]}]
  (db/find-trips-for-stop db (:stop-id params)))

(defapi trip-updates ::api/trip-updates-request
  [{:keys [db params]}]
  (let [{:keys [trip-id trip-start]} params]
    (json/write-str
     (db/trip-updates db trip-id trip-start))))

(defroutes handler
  (context "/xapi" []
           (GET "/find_stops" []  find-stops)
           (GET "/trip_performance" []  trip-performance)
           (GET "/travel_times" [] travel-times)
           (GET "/trips_for_stop" [] trips-for-stop)
           (GET "/trip_updates" [] trip-updates)))

(def app
  (-> #'handler
      (wrap-keyword-params)
      (wrap-params)))

(defn make-app [conn db]
  (fn [req]
    (app (assoc req :mongo conn, :db db))))
