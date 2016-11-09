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

(ns rammler.amqp
  (:refer-clojure :exclude [short long boolean float double type rest])
  (:require [rammler.util :refer [flagfn]]
            [gloss.core :as gloss :refer :all]
            [gloss.io :refer [decode decode-stream encode]]
            [manifold.stream :as s]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [camel-snake-kebab.extras :refer [transform-keys]]))

(def spec (json/parse-stream (io/reader (io/resource "amqp-rabbitmq-0.9.1.json")) true))

(defmulti ^:private encode-field-value
  "Dispatch on type to determine the required prefix for AMQP field tables"
  clojure.core/type)

(defmethod encode-field-value :default [x]
  (throw (ex-info "Not implemented" {:type (clojure.core/type x)
                                     :value x})))

(defmethod encode-field-value java.lang.Boolean [_] \t)

(defmethod encode-field-value java.lang.Number [n]
  (if (neg? n)
    (condp > (/ (Math/log10 (* n -2)) (Math/log10 2))
      8 \b
      16 \u
      32 \i
      64 \L)
    (condp > (/ (Math/log10 n) (Math/log10 2))
      8 \B
      16 \U
      32 \I
      64 \l)))

(defmethod encode-field-value java.lang.Float [_] \f)
(defmethod encode-field-value java.lang.Double [_] \d)
(defmethod encode-field-value java.lang.String [s]
  ; Somehow, RabbitMQ doesn't eat short strings in tables where it should be allowed
  #_(if (< (count s) 256) \s \S)
  \S)

(defmethod encode-field-value clojure.lang.PersistentHashMap [_] \F)
(defmethod encode-field-value clojure.lang.PersistentArrayMap [_] \F)
(defmethod encode-field-value clojure.lang.PersistentVector [_] \A)

;;; Grammar
(defcodec- short :uint16)
(defcodec- long :uint32)
(defcodec- octet :byte)
(defcodec- shortstr (finite-frame octet (string :ascii))
  identity
  #(or % ""))
(defcodec- longstr (finite-frame long (string :ascii))
  identity
  #(or % ""))
(defcodec- value-type octet)
(defcodec- boolean octet
  #(if % 1 0)
  #(not (zero? %)))
(defcodec- short-short-int octet)
(defcodec- short-short-uint octet)
(defcodec- short-int :int16)
(defcodec- short-uint short)
(defcodec- long-int :int32)
(defcodec- long-uint long)
(defcodec- long-long-int :int64)
(defcodec- long-long-uint :uint64)
(defcodec- float :float)
(defcodec- double :double)
(defcodec- decimal-value [octet long])
(defcodec- scale octet)
(defcodec- timestamp long-long-uint)
(defcodec- type octet)
(defcodec- channel short)
(defcodec- frame-end octet)

; Solve circular dependency table-value-types -> field-table -> field-value-pair -> field-value -> table-value-types
(declare table-value-types)
(defcodec- field-value (header octet (comp #'table-value-types char) (comp int encode-field-value)))
(defcodec- field-array (repeated field-value :prefix long-int))
(defcodec- field-name shortstr)
(defcodec- field-value-pair [field-name field-value])
(defcodec- field-table (finite-frame long-uint (repeated field-value-pair :prefix :none))
  seq
  (partial into {}))

; Exception for AMQPLAIN authentication; field table has known length
(defcodec- field-table-amqplain (repeated field-value-pair :prefix :none)
  seq
  (partial into {}))

(def ^:private table-value-types
  {\t boolean
   \b short-short-int
   \B short-short-uint
   \U short-int
   \u short-uint
   \I long-int
   \i long-uint
   \L long-long-int
   \l long-long-uint
   \f float
   \d double
   \D decimal-value
   \s shortstr
   \S longstr
   \A field-array
   \T timestamp
   \F field-table
   \V :none})

;;; Domains
(defcodec- class-id short)
(defcodec- consumer-tag shortstr)
(defcodec- delivery-tag long-long-uint)
(defcodec- exchange-name shortstr)
(defcodec- message-count long)
(defcodec- method-id short)
(defcodec- path shortstr)
(defcodec- table field-table)
(defcodec- peer-properties table)
(defcodec- reply-code short)
(defcodec- reply-text shortstr)
(defcodec- any (repeated :byte :prefix :byte))
(defcodec- bits octet)
(defcodec- longlong long-long-uint)

;;; SASL
(defcodec- sasl-plain (string :utf-8))

;;; Protocol
(defcodec amqp-header [(string :ascii :length 4) octet octet octet octet])
(defcodec- payload (repeated :byte :prefix long))
(defcodec amqp-frame [type channel payload frame-end])
(defcodec- rest (repeated :byte :prefix :none))
(defcodec- weight short)
(defcodec- body-size long-long-uint)
(defcodec- property-flags short)
(defcodec- property-list rest)
(defcodec- amqp-content-header [class-id weight body-size property-flags property-list])

(def amqp-frame-types
  "AMQP frame types

  From the Protocol Specification, 4.2.3 General Frame Format:
  AMQP defines these frame types:
  - Type = 1, \"METHOD\": method frame.
  - Type = 2, \"HEADER\": content header frame.
  - Type = 3, \"BODY\": content body frame.
  - Type = 4, \"HEARTBEAT\": heartbeat frame.

  Type 4 is wrong, it's actually 8 for heartbeat frames. The
  documentation contradicts itself later and 8 is what's actually
  implemented."
  {1 :method
   2 :header
   3 :body
   8 :heartbeat})

(def amqp-classes
  "AMQP classes class-id -> keyword

  Also see the AMQP XML specification, 1.3. Class and Method ID Summaries"
  (into {} (for [{:keys [id name]} (spec :classes)]
             [id (keyword name)])))

(def amqp-classes-reverse
  "AMQP classes keyword -> class-id"
  (zipmap (vals amqp-classes) (keys amqp-classes)))

(def amqp-methods
  "AMQP methods class -> method-id -> keyword

  Also see the AMQP XML specification, 1.3. Class and Method ID Summaries"
  (into {} (for [{:keys [name methods]} (spec :classes)]
             [(keyword name)
              (into {} (for [{:keys [id name]} methods]
                         [id (keyword name)]))])))

(def amqp-methods-reverse
  "AMQP methods

  class -> keyword -> method-id"
  (into {} (for [[class methods] amqp-methods] [class (zipmap (vals methods) (keys methods))])))

(defn- pack-bitmasks
  "Pack together arguments of type `bit`"
  [arguments]
  (mapcat (fn [coll] (let [[{:keys [type]}] coll]
                       (if (= type "bit") 
                         [{:name "bitmask" :domain "octet" :flags (map (comp keyword :name) coll) :default-value (map :default-value coll)}]
                         coll)))
          (partition-by #(= (% :type) "bit") arguments)))

(defn- arguments-codec
  "Compile codec for `arguments`"
  [arguments]
  (compile-frame
   (for [{:keys [domain type] :as argument} arguments]
     (eval (symbol (or domain type))))))

(defn- postdecoder
  "Create postdecoder from `arguments`"
  [arguments]
  (let [convert (map (fn [{:keys [name flags]}]
                       (if (= name "bitmask") (flagfn flags) identity))
                     arguments)]
    (fn [values]
      (into {} (map (fn [f {:keys [name]} value] [(keyword name) (f value)])
                    convert arguments values)))))

(def amqp-method-signatures
  "AMQP method signatures class -> method -> signature
  
  Also see the AMQP XML specification."
  (into {}
        (for [{:keys [name methods]} (spec :classes)]
          [(keyword name)
           (into {} (for [{:keys [name arguments] :as method} methods
                          :let [arguments (pack-bitmasks arguments)]]
                      [(keyword name) (merge method
                                             {:arguments arguments
                                              :fields (map (comp keyword :name) arguments)
                                              :codec (arguments-codec arguments)
                                              :postdecoder (postdecoder arguments)})]))])))

(defmulti decode-frame
  "Further decode the payload of a decoded AMQP frame

  Dispatches on frame type."
  (comp amqp-frame-types first))

(defmethod decode-frame :default [[frame-type channel payload]]
  (throw (ex-info "Unimplemented frame type" {:type (get amqp-frame-types frame-type frame-type)
                                              :channel channel
                                              :payload payload})))

(defmethod decode-frame :method [[_ channel payload]]
  (let [[class-id method-id arguments] (decode [class-id method-id rest] (byte-array payload))
        class (amqp-classes class-id)
        method (get-in amqp-methods [class method-id])
        {:keys [postdecoder codec]} (get-in amqp-method-signatures [class method])]
    {:type :method
     :channel channel
     :class class
     :method method
     :payload (try (postdecoder (decode codec (byte-array arguments)))
                   (catch Exception _ {:error arguments}))}))

(defmethod decode-frame :header [[frame-type channel payload]]
  {:type :header
   :channel channel
   :payload (zipmap
             [:class-id :weight :body-size :property-flags :property-list]
             (decode amqp-content-header (byte-array payload)))})

(defmethod decode-frame :body [[frame-type channel payload]]
  {:type :body
   :channel channel
   :payload payload})

(defmethod decode-frame :heartbeat [_] {:type :heartbeat})

(defmulti decode-frame-light
  "Partially decode the payload of a decoded AMQP frame

  Dispatches on frame type."
  (comp amqp-frame-types first))

(defmethod decode-frame-light :default [[frame-type channel payload]]
  (throw (ex-info "Unimplemented frame type" {:type (get amqp-frame-types frame-type frame-type)
                                              :channel channel
                                              :payload payload})))

(defmethod decode-frame-light :method [[_ channel payload]]
  (let [[class-id method-id payload] (decode [class-id method-id rest] (byte-array payload))
        class (amqp-classes class-id)
        method (get-in amqp-methods [class method-id])]
    {:type :method
     :channel channel
     :class class
     :method method
     :payload payload}))

(defmethod decode-frame-light :header [[frame-type channel payload]]
  {:type :header
   :channel channel
   :payload payload})

(defmethod decode-frame-light :body [[frame-type channel payload]]
  {:type :body
   :channel channel
   :payload payload})

(defmethod decode-frame-light :heartbeat [_] {:type :heartbeat})

(defn validate-frame
  "Validate decoded AMQP frame

  Checks whether `frame-end` is `0xCE` and drops it in return.
  Throws an exception otherwise."
  [[type channel payload frame-end]]
  (if (= 0xCE (bit-and frame-end 0xFF))
    [type channel payload]
    (throw (ex-info "Invalid frame-end byte encountered" {:frame-end frame-end}))))

(defmulti encode-frame
  "Encode AMQP frame based on frame type"
  :type)

(defmethod encode-frame :method [{:keys [channel class method payload]}]
  (let [id [(amqp-classes-reverse class) (get-in amqp-methods-reverse [class method])]
        {:keys [fields codec]} (get-in amqp-method-signatures [class method])]
    (encode amqp-frame [1 channel
                        (decode rest
                                (concat (encode [class-id method-id] id)
                                        (encode codec (map payload fields))))
                        (.byteValue 0xCE)])))

(defmulti postdecode
  "Post-decoding operations on frames"
  (fn [{:keys [class method]}] [class method]))

(defmethod postdecode :default [x] x)

(let [null-regex (re-pattern (str (char 0)))]
  (defmethod postdecode [:connection :start-ok] [{:keys [payload] :as frame}]
    (case (payload :mechanism)
      "PLAIN" (update-in frame [:payload :response]
                #(zipmap [:cid :login :password] (str/split % null-regex)))
      "AMQPLAIN" (update-in frame [:payload :response]
                   #(transform-keys ->kebab-case-keyword
                      (decode field-table-amqplain
                        (byte-array (decode rest %)))))
      (throw (ex-info (payload :mechanism) {:cause :unsupported-authentication-type})))))

(defn decode-amqp-frame
  "Fully decode AMQP `frame`"
  [frame]
  (try (postdecode (decode-frame (validate-frame frame)))
       (catch Exception e
         (throw (ex-info "Error decoding frame" {:cause :frame-decoding-error} e)))))

(defn decode-amqp-frame-light
  "Partially decode AMQP `frame`"
  [frame]
  (try (decode-frame-light (validate-frame frame))
       (catch Exception e
         (throw (ex-info "Error decoding frame" {:cause :frame-decoding-error} e)))))

(defn decode-amqp-stream
  "Return new gloss stream from `src` that emits decoded frames"
  [src]
  (s/map decode-amqp-frame (decode-stream src amqp-frame)))

(defn decode-amqp-stream-light
  "Return new gloss stream from `src` that emits partially decoded frames together with the raw bytes"
  [src]
  (s/map
    (fn [frame]
      [(decode-amqp-frame-light frame)
       (encode amqp-frame frame)])
    (decode-stream src amqp-frame)))
