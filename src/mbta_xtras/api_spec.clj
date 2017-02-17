(ns mbta-xtras.api-spec
  "Specs for "
  (:require [clojure.spec :as s]
            [clojure.string :as str]
            [clojure.data.json :as json]

            [mbta-xtras.spec :as xs])
  (:import [java.time ZoneId]))

(s/def ::positive-int-str #(re-matches #"\d+"))
(s/def ::float-str #(re-matches #"-?\d+(\.\d+)" %))
(s/def ::timezone (s/and string? #(contains? (ZoneId/getAvailableZoneIds) %)))
(s/def ::q string?)
(s/def ::lat (s/and ::float-str #(<= -90 (Float/parseFloat %) 90)))
(s/def ::lon (s/and ::float-str #(<= -180 (Float/parseFloat %) 190)))
(s/def ::direction ::positive-int-str)
(s/def ::route-id string?)
(s/def ::route ::route-id)
(s/def ::stop-id string?)
(s/def ::trip-id string?)
(s/def ::trip-start ::xs/date-str)
(s/def ::timestamp-str (s/and string? #(re-matches #"^\d+$" %)))

;; User requests:
(s/def ::q string?)
(s/def ::find-stops-query
  (s/keys :req-un [(or ::q (and ::lat ::lon) (and ::q ::lat ::lon))]))

(s/def ::trip-performance-request
  (s/keys :req-un [::trip-id ::trip-start]))

;; Travel times:
(s/def ::from-datetime ::timestamp-str)
(s/def ::to-datetime ::timestamp-str)
(s/def ::from-stop ::stop-id)
(s/def ::to-stop ::stop-id)
(s/def ::travel-times-request
  (s/keys :req-un [::from-datetime ::to-datetime ::from-stop ::to-stop]
          :opt-un [::route]))

(s/def ::dep-dt ::positive-int-str)
(s/def ::arr-dt ::positive-int-str)
(s/def ::travel-time-sec ::positive-int-str)
(s/def ::benchmark-travel-time-sec ::positive-int-str)
(s/def ::threshold-flag-1 string?)
(s/def ::threshold-flag-2 string?)
(s/def ::threshold-flag-3 string?)
(s/def ::travel-time
  (s/keys :req-un [::route-id ::direction ::dep-dt ::arr-dt
                   ::travel-time-sec ::benchmark-travel-time-sec]
          :opt-un [::threshold-flag-1 ::threshold-flag-2
                   ::threshold-flag-3]))
(s/def ::travel-times
  (s/coll-of ::travel-time))
(s/def ::travel-times-response
  (s/keys :req-un [::travel-times]))

;; Dwell times:
(s/def ::dwells-request
  (s/keys :req-un [::from-datetime ::to-datetime ::stop-id]
          :opt-un [::route ::direction]))

(s/def ::trips-for-stop-request
  (s/keys :req-un [::stop-id]))

(s/def ::trip-updates-request
  (s/keys :req-un [::trip-id ::trip-start]))
(s/def ::trip-updates-response
  (s/keys :req-un []))

;; Headways
(s/def ::stop ::stop-id)
(s/def ::route ::route-id)
(s/def ::headways-request
  (s/keys :req-un [::stop ::from-datetime ::to-datetime]
          :opt-un [::to-stop ::route]))

;; Daily Metrics
(s/def ::daily-metrics-request
  map?)


;; Services request
(s/def ::services-request
  (s/or :at (s/keys :req-un [::at])
        :date-str (s/keys :req-un [::xs/date-str])))

(s/def ::services-request
  (s/keys :req-un [::at]))

(s/def ::day ::xs/date-str)
(s/def ::at ::xs/time-str)
(s/def ::timestamp ::timestamp-str)
(s/def ::trips-at-request
  (s/or :date-time (s/keys :req-un [::day ::at])
        :stamp (s/keys :req-un [::timestamp])))

(s/def ::current-metrics-request
  map?)


;; Utilities:
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

(defn keyfn [x]
  (str/replace (if (keyword? x) (name x) (str x))
               #"-" "_"))

(defn wrap-json [x]
  (cond
    (:status x) x

    :else
    {:status 200
     :headers {"Content-type" "application/json"}
     :body (if (string? x) x (json/write-str x))}))

(defn translate-param-key [k]
  (-> (name k)
      (str/replace "_" "-")
      (keyword)))

(def translate-params-xf
  (map (fn [[k v]] [(translate-param-key k) v])))

(defn api-endpoint [spec f]
  (fn [{:keys [params] :as req}]
    (let [params (into {} translate-params-xf params)
          conformed (s/conform spec params)]
      (if (= conformed ::s/invalid)
          ;; Respond with a user error if the request params do not conform to the
          ;; API specification.
          {:status 400
           :headers {"Content-type" "application/json"}
           :body (json/write-str {:errors (error-message (s/explain-data spec params))
                                  :request-params params}
                                 :key-fn keyfn)}

          (wrap-json (f (assoc req :conformed conformed
                               :params params)))))))

(defmacro defapi [name spec params & body]
  `(def ~name (api-endpoint ~spec (fn ~params ~@body))))
