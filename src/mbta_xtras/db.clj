(ns mbta-xtras.db
  (:require [monger.collection :as mc]
            [monger.core :as mg]
            [monger.operators :refer [$near]]

            [mbta-xtras.gtfs :as gtfs]
            [mbta-xtras.realtime :as rt]
            [clojure.core.async :as async :refer [<! go-loop]]
            [clojure.string :as str]
            [taoensso.timbre :refer [info]]
            [mbta-xtras.db :as db]
            [mbta-xtras.utils :as $]))

(defonce agencies (atom nil))
(defonce transfers (atom nil))
(defonce calendar (atom nil))

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

(defn find-closest-stop [db lat lon]
  (mc/find-one-as-map db "stops" {:coords (near-query lat lon)}))

(defn double-key [k] #(Double/parseDouble (k %)))

;; Stop times
(defn process-stop-time [x]
  (update x :stop-sequence #(Integer/parseInt %)))

(defn save-api-trip! [db api-trip]
  (mc/insert db "trips" (dissoc api-trip :stops))
  (mc/insert-batch db "stop-times" (:stops api-trip)))

(defn stop-times-for-trip [db trip-id]
  (let [stop-times (mc/find-maps db "stop-times" {:trip-id trip-id}
                                 {:_id 0, :stop-id 1, :stop-sequence 1, :arrival-time 1,
                                  :departure-time 1})]
    (if (seq stop-times)
      stop-times

      (when-let [api-trip (rt/get-trip trip-id)]
        (save-api-trip! api-trip)
        (:stops api-trip)))))

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

(defn collection-swap [db coll-name docs]
  (if (mc/exists? db coll-name)
    (do (mc/insert-batch db (str "new-" coll-name) docs)
        (mc/rename db coll-name (str coll-name "-" ($/today-str)))
        (mc/rename db (str "new-" coll-name) coll-name))
    (mc/insert-batch db coll-name docs)))

(defmulti process-csv! first)
(defmethod process-csv! nil [_] nil)
(defmethod process-csv! "stops.txt"
  [[_ stops] db]
  (collection-swap db "stops" (map process-stop stops)))

(defmethod process-csv! "routes.txt"
  [[_ routes] db]
  (collection-swap db "routes" routes))

(defmethod process-csv! "stop_times.txt"
  [[_ stop-times] db]
  (doseq [stop-times-group (partition 20000 (map process-stop-time stop-times))]
    (mc/insert-batch db "new-stop-times" stop-times-group))
  (when (mc/exists? db "stop-times")
    (mc/rename db (str "stop-times-" ($/today-str))))
  (mc/rename db "new-stop-times" "stop-times"))

(defmethod process-csv! "trips.txt"
  [[_ trips] db])

(defmethod process-csv! "calendar.txt"
  [[_ service-dates] _db]
  (reset! calendar service-dates))

(defmethod process-csv! "agencies.txt"
  [[_ new-agencies] _db]
  (reset! agencies new-agencies))

(defmethod process-csv! "transfers.txt"
  [[_ new-transfers] _db]
  (reset! transfers new-transfers))

;; Ignore: transfers.txt, agencies.txt, calendar_dates.txt, 

(defn update-loop
  "Start a feed updates channel. Every time it reports an update, "
  []
  (let [updates (gtfs/feed-updates)]
    (go-loop []
      ;; The updates channel receives a non-nil message each time the feed is
      ;; updated.
      (when (<! updates)
        (doseq [[file-name _csv :as gtfs-file] (gtfs/csv-to-maps)]
          (info "Reading updates from " file-name)
          (process-csv! gtfs-file))))))

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
  "Determines if the trip `trip-id` runs on the date `dt`."
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
