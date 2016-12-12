(ns mbta-xtras.gtfs-updater
  (:require [clojure.core.async :refer [<! alt! chan go-loop timeout]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn]]

            [mbta-xtras.gtfs :as gtfs]
            [clojure.java.io :as io])
  (:import [java.util.zip ZipInputStream ZipEntry]))


(def *zis (ZipInputStream. (io/input-stream gtfs/manifest-url)))

(def *entry (.getNextEntry *zis))

(defonce feed-info (atom nil))

(defn check-feed-info!
  "Checks if the GTFS manifest has been updated. Returns the latest info if it
  has changed."
  []
  (let [latest (gtfs/get-latest-feed-info)]
    (when (not= (:feed-version @feed-info) (:feed-version latest))
      (reset! feed-info latest))))

(defn update-loop []
  (let [stop (chan)]
    (go-loop []
      (when (check-feed-info!)
        ;; Update everything!
        )
      (<! (timeout 86400000)))))

(defrecord GtfsUpdater []
  component/Lifecycle
  (start [this]
    ))
