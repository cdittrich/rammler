(ns rammler.amqp
  (:refer-clojure :exclude [short long boolean float double type rest])
  (:require [rammler.util :refer :all]
            [gloss.core :as gloss :refer :all]
            [gloss.io :refer [decode decode-stream encode]]
            [manifold.stream :as s]
            [clojure.string :as str]

            [clojure.java.io :as io]
            [cheshire.core :as json]))

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

(comment
  (into {}
        (for [{:keys [name methods]} (amqp/spec :classes)]
          [(keyword name)
           (into {} (for [{:keys [name arguments]} methods]
                      [(keyword name) (for [{:keys [name type]} arguments] [(keyword name) type])]))])))

(def amqp-method-signatures
  "AMQP method signatures class -> method -> signature sequence

  Each signature tuple consists of `[field-name codec]'.

  Also see the AMQP XML specification."
  {:connection
   {:start [[:version-major octet] [:version-minor octet] [:server-properties peer-properties] [:mechanisms longstr] [:locales longstr]]
    :start-ok [[:client-properties peer-properties] [:mechanism shortstr] [:response longstr] [:locale shortstr]]
    :secure [[:challenge longstr]]
    :secure-ok [[:response longstr]]
    :tune [[:channel-max short] [:frame-max long] [:heartbeat short]]
    :tune-ok [[:channel-max short] [:frame-max long] [:heartbeat short]]
    :open [[:virtual-host path] [:reversed-1 any] [:reserved-2 any]]
    :open-ok [[:reserved-1 rest]]
    :close [[:reply-code reply-code] [:reply-text reply-text] [:class-id class-id] [:method-id method-id]]
    :close-ok []}

   :channel
   {:open [[:virtual-host path] #_[:reversed-1 any] #_[:reserved-2 any]]
    :open-ok [[:reversed-1 rest]]
    :close [[:reply-code reply-code] [:reply-text reply-text] [:class-id class-id] [:method-id method-id]]
    :close-ok []}

   :basic
   {:publish [#_[:reserved-1 any] [:trash-1 octet] [:trash-2 octet] [:exchange exchange-name] [:routing-key shortstr] [:attributes bits] #_[:mandatory bit] #_[:immediate bit]]}})

(def amqp-method-fields
  "Remapping of AMQP method signatures to fields signature -> fields"
  (into {} (for [[class-id class] amqp-classes [method-id method] (amqp-methods class)
                 :let [signature (get-in amqp-method-signatures [class method])]]
             [[class-id method-id] (if signature (map first signature) :not-implemented)])))

(def amqp-method-codecs
  "Remapping of AMQP method signatures to frames signature -> codec"
  (into {} (for [[class-id class] amqp-classes [method-id method] (amqp-methods class)]
             [[class-id method-id] (compile-frame (map second (get-in amqp-method-signatures [class method])))])))

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
        method [class-id method-id]
        fields (amqp-method-fields method)]
    {:type :method
     :channel channel
     :class class
     :method (get-in amqp-methods [class method-id])
     :payload (if (= fields :not-implemented)
                {:not-implemented arguments}
                (try (zipmap fields (decode (amqp-method-codecs method) (byte-array arguments)))
                     (catch Exception _ {:error arguments})))}))

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

(defmethod decode-frame :heartbeat [_] {:type :heatbeat})

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
  (let [id [(amqp-classes-reverse class) (get-in amqp-methods-reverse [class method])]]
    (encode amqp-frame [1 channel
                        (decode rest
                                (concat (encode [class-id method-id] id)
                                        (encode (amqp-method-codecs id)
                                                (map payload (amqp-method-fields id)))))
                        (.byteValue 0xCE)])))

(defmulti postdecode
  "Post-decoding operations on frames"
  (fn [{:keys [class method]}] [class method]))

(defmethod postdecode :default [x] x)

(let [null-regex (re-pattern (str (char 0)))]
  (defmethod postdecode [:connection :start-ok] [frame]
    (update-in frame [:payload :response]
               #(zipmap [:authcid :authzid :passwd] (str/split % null-regex)))))

(defmethod postdecode [:basic :publish] [frame]
  (update-in frame [:payload :attributes] (flagfn :mandatory :immediate)))

(defn decode-amqp-frame
  "Fully decode AMQP `frame`"
  [frame]
  (try (postdecode (decode-frame (validate-frame frame)))
       (catch Exception e
         (println "Error decoding frame: " e)
         {})))

(defn decode-amqp-stream
  "Return new gloss stream from `src` that emits decoded frames"
  [src]
  (s/map decode-amqp-frame (decode-stream src amqp-frame)))
