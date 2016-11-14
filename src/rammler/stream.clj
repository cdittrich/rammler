;;;; rammler AMQP proxy
;;;; Copyright (C) 2016  LShift Services GmbH
;;;; 
;;;; This program is free software: you can redistribute it and/or modify
;;;; it under the terms of the GNU Affero General Public License as
;;;; published by the Free Software Foundation, either version 3 of the
;;;; License, or (at your option) any later version.
;;;; 
;;;; This program is distributed in the hope that it will be useful,
;;;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;;;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;;; GNU Affero General Public License for more details.
;;;; 
;;;; You should have received a copy of the GNU Affero General Public License
;;;; along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns rammler.stream
  (:require [manifold.deferred :as d]
            [manifold.stream :as s]))

(defn async-stream
  "Provide a decoding stream from `src` that pushes messages through agent `a`"
  [src a]
  (let [dst (s/stream)]
    (s/connect-via src (fn [frame] (send a (fn [_] (s/put! dst frame))) d/true-deferred-) dst)
    (s/on-drained src #(s/close! dst))
    (s/source-only dst)))

(defn timer
  "manifold stream that emits `hertz` `:token`s per second.

  Charges up to `burst` messages when not consumed."
  ([hertz burst]
   (s/buffer burst (s/periodically (int (* (/ 1 hertz) 1000)) (constantly :token))))
  ([hertz] (timer hertz hertz)))

(defn queued
  "Return queued stream from `source`

  Whenever a value can be taken from `timer`, emit a message.
  If `weightfn` is provided, fetch `(weightfn msg)` tokens instead."
  ([source timer] (queued source timer (constantly 1)))
  ([source timer weightfn]
   (let [sink (s/stream)]
     (s/connect-via source
       (fn [msg] (d/chain (d/loop [i (weightfn msg)]
                            (when (> i 0)
                              (d/chain (s/take! timer)
                                (fn [_] (d/recur (dec i))))))
                   (fn [_] (s/put! sink msg))))
       sink)
     sink)))
