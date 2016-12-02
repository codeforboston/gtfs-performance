(ns mbta-xtras.realtime
  "Code for interacting with the realtime API."
  (:require [clojure.spec :as s]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [environ.core :refer [env]]
            [clojure.data.json :as json]

            [mbta-xtras.spec :as xs]
            [mbta-xtras.utils :as $]))

(s/fdef get-trip
        :args (s/cat :trip-id ::xs/trip-id)
        :ret ::xs/trip-description)

(defprotocol ExternalTripInfo
  (get-trip-with [x trip-id]))

(defn url-encode [s]
  (java.net.URLEncoder/encode s "UTF-8"))

(defn encode-params [m & [pref]]
  (str/join "&" (map (fn [[k v]]
                       (if (map? v)
                         (encode-params v (str (url-encode (name k)) "."))
                         (str pref (url-encode (name k)) "="
                              (url-encode (str v)))))
                     m)))

(defn convert-json [v]
  (cond
    (map? v) (into {} (map (fn [[k v]]
                             [(keyword (str/replace k #"[_\s]+" "-"))
                              (convert-json v)]))
                   v)
    (coll? v) (mapv convert-json v)
    :else v))

(defmacro map-from [x & {:as  k-exprs}]
  (let [l (gensym)]
    `(let [~l ~x]
       (hash-map ~@(mapcat (fn [[k expr]]
                             (list k `(-> ~l ~expr)))
                           k-exprs)))))

;; "direction-id" : "0",
;; "service-id" : "CapeFlyer-Friday-2016",
;; "trip-headsign" : "Hyannis",
;; "trip-id" : "CapeFlyer-Friday-2016-9001",
;; "block-id" : "",
;; "shape-id" : "cf00002",
;; "route-id" : "CapeFlyer",
;; "wheelchair-accessible" : "1",
;; "trip-short-name" : "9001"


;; MBTA
(def mbta-base-url (java.net.URL. "http://realtime.mbta.com/developer/api/v2/"))


(defn convert-stop [stop]
  (map-from stop
            :stop-sequence (-> :stop-sequence Integer/parseInt)
            :arrival-time :sch-arr-dt
            :departure-time :sch-dep-dt))

(defn mbta-url [path & [params]]
  (java.net.URL. mbta-base-url (str path (when params (str "?" (encode-params params))))))

(defrecord MBTATripInfo [api-key]
  ExternalTripInfo
  (get-trip-with [this trip-id]
    (try
      (-> (mbta-url "schedulebytrip" {:api_key api-key
                                      :trip trip-id})
          (io/reader)
          (json/read)
          (convert-json))

      (catch java.io.FileNotFoundException exc
        nil))))

(defn get-mbta []
  ;; Use the public API key by default.
  (->MBTATripInfo (env :mbta-api-key "wX9NwuHnZU2ToO7GmGR9uw")))

(defn get-external []
  (case (env :external-api "mbta")
    "mbta" (get-mbta)))

(defn get-trip-raw [trip-id]
  (get-trip-with (get-external) trip-id))

(defn headsign-from-name [trip-name]
  (last (str/split trip-name #" to ")))

(defn process-trip
  "Converts the API response trip into a trip suitable for storing in the
  database."
  [trip]
  (let [start-date (-> (:stop trip)
                       (first)
                       :sch-arr-dt
                       (Integer/parseInt)
                       ($/date-for-stamp))
        make-sch #($/->clock-time start-date (Integer/parseInt %))
        stops (:stop trip)]
    (-> trip
        (dissoc :stop)
        (assoc
         :trip-headsign (headsign-from-name (:trip-name trip))
         :stops (map (fn [stop]
                       (-> stop
                           (update :stop-sequence #(Integer/parseInt %))
                           (dissoc :sch-arr-dt :sch-dep-dt)
                           (assoc :scheduled-arrival (make-sch (:sch-arr-dt stop))
                                  :scheduled-departure (make-sch (:sch-dep-dt stop)))))
                     stops)))))

(defn get-trip [trip-id]
  (some-> (get-trip-raw trip-id) (process-trip)))

