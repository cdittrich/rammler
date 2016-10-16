(ns rammler.stats
  (:require [rammler.elastic :as es]
            [clj-time.core :as t]
            [clj-time.periodic :as tp]
            [clojure.core.async :refer [go go-loop alts! close! <!! chan <! >!! >!]]
            [chime :refer [chime-ch]]))

(taoensso.timbre/refer-timbre)

(defonce stats (ref {}))

(defn init-account! [login]
  (dosync (alter stats update login (partial merge {:publish [] :deliver []}))))

(defn reset-account! [login]
  (dosync (alter stats assoc login {:publish [] :deliver []})))

(defn purge-stats! []
  (dosync (doseq [[login] @stats] (reset-account! login))))

(defn- timestamp [] (System/currentTimeMillis))

(defn register! [login q addr]
  (dosync (alter stats update-in [login q] conj {:timestamp (timestamp) :addr addr})))

(defmulti handle-method-frame (fn [login addr {:keys [type class method]}] [type class method]))

(defmethod handle-method-frame :default [login addr frame])

(defmethod handle-method-frame [:method :basic :publish] [login addr _]
  (register! login :publish addr))

(defmethod handle-method-frame [:method :basic :deliver] [login addr _]
  (register! login :deliver addr))

(defn handler [login addr] (partial handle-method-frame login addr))

(defn record-stats! []
  (info "Recording current stats")
  (dosync
    (doseq [[login data] @stats
           [type events] data
           [addr events] (group-by :addr events)
           :let [events (sort-by :timestamp < events)]]
      (es/register! login {:from (:timestamp (first events)) :to (:timestamp (last events)) :addr addr :type (name type) :count (count events)}))
    (purge-stats!)))

(def stop-channel (chan))

(defn start-scheduler []
  (info "Starting stats scheduler")
  (let [chimes (chime-ch (rest (tp/periodic-seq (t/now) (t/minutes 5))))]
    (go-loop []
      (let [[msg ch] (alts! [chimes stop-channel])]
        (when (= ch stop-channel)
          (close! chimes))
        (debug "Running schedule")
        (record-stats!)
        (recur)))))

(defn stop-scheduler []
  (>!! stop-channel true))
