(ns mbta-xtras.handler
  (:require [clojure.data.json :as json]
            [compojure.core :refer [context defroutes GET POST]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.params :refer [wrap-params]]

            [mbta-xtras.api-spec :as api :refer [defapi keyfn]]
            [mbta-xtras.db :as db]
            [mbta-xtras.trip-performance :as trip]

            [clojure.spec :as s]
            [clojure.string :as str]
            [mbta-xtras.utils :as $]))


(defapi find-stops ::api/find-stops-query
  [{:keys [db params]}]
  (db/find-stops db params))

(defapi trip-performance ::api/trip-performance-request
  [{:keys [db params]}]
  (let [results (trip/observed-trip-performance db
                                                (:trip-id params)
                                                (:trip-start params))]
    {:performance results}))

(defapi travel-times ::api/travel-times-request
  [{:keys [db params]}]
  (let [{:keys [from-datetime to-datetime
                from-stop to-stop]} params]
    {:travel-times (trip/travel-times db from-stop to-stop
                                      (Long/parseLong from-datetime)
                                      (Long/parseLong to-datetime))}))

(defapi dwells ::api/dwells-request
  [{:keys [db params]}]
  {:status 501
   :body "Not implemented"})

(defapi headways ::api/headways-request
  [{:keys [db params]}]
  {:status 501
   :body "Not implemented"})

(defapi daily-metrics ::api/daily-metrics-request
  [_]
  {:status 501
   :body "Not implemented"})

(defapi current-metrics ::api/current-metrics-request
  [_]
  {:status 501
   :body "Not implemented"})

(defapi trips-for-stop ::api/trips-for-stop-request
  [{:keys [db params]}]
  (db/find-trips-for-stop db (:stop-id params)))

(defapi trip-updates ::api/trip-updates-request
  [{:keys [db params]}]
  (let [{:keys [trip-id trip-start]} params]
    (json/write-str
     (db/trip-updates db trip-id trip-start)
     :key-fn keyfn)))

(defapi services-at ::api/services-request
  [{:keys [db params]}]
  (let [dt ($/datetime-for-stamp (Integer/parseInt (:at params)))]
    (case (db/scheduled-trips-at db dt))))

(defapi trips-at ::api/trips-at-request
  [{:keys [db conformed]}]
  (let [conform-key (first conformed)
        {:keys [day at stamp]} (second conformed)
        dt (case conform-key
             ::api/date-time (.. ($/date-for-str day) (atTime ($/time-for-str at)))
             ::api/stamp ($/datetime-for-stamp (Integer/parseInt at)))]
    (db/scheduled-trips-at db dt)))

(defroutes handler
  (GET "/find_stops" []  find-stops)
  (GET "/trips_for_stop" [] trips-for-stop)
  (GET "/trip_performance" []  trip-performance)
  (GET "/services" [] services-at)
  (GET "/trips_at" [] trips-at)

  ;; MBTA Performance API:
  (GET "/dwells" [] dwells)
  (GET "/headways" [] headways)
  (GET "/traveltimes" [] travel-times)
  (GET "/trip_updates" [] trip-updates))

(def app
  (-> #'handler
      (wrap-keyword-params)
      (wrap-params)))

(defn make-app [conn db]
  (fn [req]
    (app (assoc req :mongo conn, :db db))))
