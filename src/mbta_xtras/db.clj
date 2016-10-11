(ns mbta-xtras.db
  (:require [monger.collection :as mc]
            [monger.core :as mg]
            [monger.operators :refer [$near]]

            [mbta-xtras.gtfs :as gtfs]))

;; (def *mongo-uri
;;   "mongodb://mbtafyi:3KIJ6Hpb0FXJleeJsRKv1PhbaYblD7ca9H5qO9UvrIoyKHbnFfTFpTZtsTsTuFfLlNUZpbJSmXqaqGsKh3pQ9Q==@mbtafyi.documents.azure.com:10250/mbta?ssl=true")
;; (def *mongo (mg/connect-via-uri *mongo-uri))
;; (def *db (:db *mongo))

(defn get-db [conn]
  (mg/get-db conn "mbta"))

(defn make-point [m lat-k lon-k]
  {:type "Point",
   :coordinates [(Double/parseDouble (lon-k m))
                 (Double/parseDouble (lat-k m))]})

(defn near-query [lat lon]
  {$near {"$geometry" {:type "Point"
                       :coordinates [lon lat]}
          "$maxDistance" 50}})

; Stops:
(defn drop-stops! [db]
  (mc/drop db "stops"))

(defn process-stop [stop]
  (assoc stop :coords (make-point stop :stop-lat :stop-lon)))

(defn insert-stops! [db]
  (mc/insert-batch db "stops"
                   (map process-stop (gtfs/get-stops))))

(defn find-stop [db]
  (mc/find-maps db "stops" {:stop-name (re-find #"Ware")}))

(defn find-closest-stop [db lat lon]
  (mc/find-one-as-map db "stops" {:coords (near-query lat lon)}))

(defn double-key [k] #(Double/parseDouble (k %)))

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

;; Trips
(defn insert-trips! [db]
  (mc/insert-batch db "trips"
                   (map process-trip (gtfs/get-trips))))


(comment
  (setup-db *db)
  (mc/find-maps *db "stops" {:stop-name #"Harvard"})
  (find-closest-stop *db 42.37336 -71.1189)
  (insert-stops! *db))
