(ns mbta-xtras.db
  (:require [monger.collection :as mc]
            [monger.core :as mg]
            [monger.operators :refer [$near]]

            [mbta-xtras.gtfs :as gtfs]
            [mbta-xtras.realtime :as rt]
            [clojure.string :as str]
            [mbta-xtras.db :as db]))


(defn get-db [conn]
  (mg/get-db conn "mbta"))

(defn make-point [m lat-k lon-k]
  {:type "Point",
   :coordinates [(Double/parseDouble (lon-k m))
                 (Double/parseDouble (lat-k m))]})

(defn near-query
  ([lat lon & [max-dist]]
   {$near {"$geometry" {:type "Point"
                        :coordinates [lon lat]}
           "$maxDistance" (or max-dist 50)}}))

; Stops:
(defn drop-stops! [db]
  (mc/drop db "stops"))

(defn process-stop [stop]
  (-> stop
      (assoc :coords (make-point stop :stop-lat :stop-lon))
      (update :stop-lat #(Double/parseDouble %))
      (update :stop-lon #(Double/parseDouble %))))

(defn insert-stops! [db]
  (mc/insert-batch db "stops"
                   (map process-stop (gtfs/get-stops))))

(defn find-closest-stop [db lat lon]
  (mc/find-one-as-map db "stops" {:coords (near-query lat lon)}))

(defn double-key [k] #(Double/parseDouble (k %)))

;; Stop times
(defn process-stop-time [x]
  (update x :stop-sequence #(Integer/parseInt %)))

(defn insert-stop-times! [db]
  (doseq [stop-times-group (partition 20000 (map process-stop-time
                                                 (gtfs/get-stop-times)))]
    (mc/insert-batch db "stop-times" stop-times-group)))

(defn stop-times-for-trip [db trip-id]
  (let [stop-times (mc/find-maps db "stop-times" {:trip-id trip-id}
                                 {:_id 0, :stop-id 1, :stop-sequence 1, :arrival-time 1})]
    (if (seq stop-times)
      stop-times

      (when-let [api-trip (rt/get-trip trip-id)]
        ))))

(defn drop-trip-stops! [db]
  (mc/drop db "trip-stops"))

;; Shapes
(defn make-shapes [shape-points]
  (map (fn [points]
         {:shape-id (:shape-id (first points))
          :path {:type "LineString"
                 :coordinates (mapv (juxt (double-key :shape-pt-lon)
                                          (double-key :shape-pt-lat))
                                    points)}})
       (partition-by :shape-id shape-points)))

(defn insert-shapes! [db]
  (mc/insert-batch db "shapes" (make-shapes (gtfs/get-shapes))))

(defn drop-shapes! [db]
  (mc/drop db "shapes"))

;; Trips
(defn drop-trips! [db]
  (mc/drop db "trips"))

(def process-trip identity)

(defn insert-trips! [db]
  (mc/insert-batch db "trips"
                   (map process-trip (gtfs/get-trips))))


(defn drop-all! [db]
  (doseq [coll ["stops" "trips" "shapes"]]
    (mc/drop db coll)))

(defn rebuild! [db]
  (doto db
    (drop-all!)
    (insert-stops!)
    (insert-shapes!)
    (insert-trips!)))

(defn make-re [s]
  (re-pattern (str "(?<=\\b|^)"
                   (str/replace s #"[\[\\.?\^\(]" "\\\\$0"))))


(defn build-stop-query [{:keys [q lat lon dist]}]
  (cond-> {}
    q (assoc :stop-name (make-re q))

    (and lat lon) (assoc :coords (near-query
                                  (Double/parseDouble lat)
                                  (Double/parseDouble lon)
                                  (Integer/parseInt dist)))))

(defn find-stops [db params]
  (mc/find-maps db "stops"
                (build-stop-query params)
                {:_id 0}))

(defn build-trip-query [{:keys []}])

(defn find-trips [db params]
  (mc/find-maps db "trips" (build-trip-query params)))

(defn trip-runs-on?
  "Determines "
  [db trip-id dt]
  (let [{:keys [service-id]} (mc/find-one-as-map db "trips" {:trip-id trip-id})]))


(defn find-trips-for-stop [db stop-id]
  (mc/distinct db "stop-times" :trip-id {:stop-id stop-id}))

(defn trip-updates [db trip-id trip-start]
  (->> (mc/find-maps db "trip-stops" {:trip-id trip-id
                                      :trip-start trip-start}
                     {:_id 0})
       (sort-by :stop-sequence)))

(defn travel-times [db from-stop]
  (mc/find-maps db "stop-times" {:stop-id from-stop} {:_id 0,
                                                      :trip-id 1
                                                      :stop-sequence 1})
  )
