(ns rammler.throttle
  (:require [rammler.stream :as rs]))

(def timers (atom {}))

(defn timer [user hertz]
  (or (get-in @timers [user hertz])
    (let [t (rs/timer hertz)]
      (swap! timers assoc-in [user hertz] t)
      t)))
