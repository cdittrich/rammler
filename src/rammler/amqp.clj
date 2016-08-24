(ns rammler.amqp
  (:refer-clojure :exclude [short long boolean float double type rest])
  (:require [gloss.core :as gloss :refer :all]
            [gloss.io :refer [decode encode]]
            [clojure.string :as str]))

(defmulti encode-field-value clojure.core/type)
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
(defcodec short :uint16)
(defcodec long :uint32)
(defcodec octet :byte)
(defcodec shortstr (finite-frame octet (string :ascii)))
(defcodec longstr (finite-frame long (string :ascii)))
(defcodec value-type octet)
(defcodec boolean octet
  #(if % 1 0)
  #(not (zero? %)))
(defcodec short-short-int octet)
(defcodec short-short-uint octet)
(defcodec short-int :int16)
(defcodec short-uint short)
(defcodec long-int :int32)
(defcodec long-uint long)
(defcodec long-long-int :int64)
(defcodec long-long-uint :uint64)
(defcodec float :float)
(defcodec double :double)
(defcodec decimal-value [octet long])
(defcodec scale octet)
(defcodec timestamp long-long-uint)
(defcodec type octet)
(defcodec channel short)
(defcodec frame-end octet)

 ; Solve circular dependency table-value-types -> field-table -> field-value-pair -> field-value -> table-value-types
(declare table-value-types)
(defcodec field-value (header octet (comp #'table-value-types char) (comp int encode-field-value)))
(defcodec field-array (repeated field-value :prefix long-int))
(defcodec field-name shortstr)
(defcodec field-value-pair [field-name field-value])
(defcodec field-table (finite-frame long-uint (repeated field-value-pair :prefix :none))
  seq
  (partial into {}))

(def table-value-types
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
(defcodec class-id short)
(defcodec consumer-tag shortstr)
(defcodec delivery-tag long-long-uint)
(defcodec exchange-name shortstr)
(defcodec message-count long)
(defcodec method-id short)
(defcodec path shortstr)
(defcodec table field-table)
(defcodec peer-properties table)
(defcodec reply-code short)
(defcodec reply-text shortstr)
(defcodec any (repeated :byte :prefix :byte))

;;; SASL
(defcodec sasl-plain (string :utf-8))

;;; Protocol
(def amqp-frame-types
  {1 :method
   2 :header
   4 :body
   8 :heartbeat})

(def amqp-classes
  {10 :connection
   20 :channel
   40 :exchange
   50 :queue
   60 :basic
   90 :tx})

(def amqp-classes-reverse
  (zipmap (vals amqp-classes) (keys amqp-classes)))

(def amqp-methods
  {:connection
   {10 :start
    11 :start-ok
    20 :secure
    21 :secure-ok
    30 :tune
    31 :tune-ok
    40 :open
    41 :open-ok
    50 :close
    51 :close-ok}
   :channel
   {10 :open
    11 :open-ok
    20 :flow
    21 :flow-ok
    40 :close
    41 :close-ok}
   :exchange
   {10 :declare
    11 :declare-ok
    20 :delete
    21 :delete-ok}
   :queue
   {10 :declare
    11 :declare-ok
    20 :bind
    21 :bind-ok
    50 :unbind
    51 :unbind-ok
    30 :purge
    31 :purge-ok
    40 :delete
    41 :delete-ok}
   :basic
   {10 :qos
    11 :qos-ok
    20 :consume
    21 :consume-ok
    30 :cancel
    31 :cancel-ok
    40 :publish
    50 :return
    60 :deliver
    70 :get
    71 :get-ok
    72 :get-empty
    80 :ack
    90 :reject
    100 :recover-async
    110 :recover
    111 :recover-ok}
   :tx
   {10 :select
    11 :select-ok
    20 :commit
    21 :commit-ok
    30 :rollback
    31 :rollback-ok}})

(def amqp-methods-reverse
  (into {} (for [[class methods] amqp-methods] [class (zipmap (vals methods) (keys methods))])))

(def amqp-method-signatures
  {:connection
   {:start [[:version-major octet] [:version-minor octet] [:server-properties peer-properties] [:mechanisms longstr] [:locales longstr]]
    :start-ok [[:client-properties peer-properties] [:mechanism shortstr] [:response longstr] [:locale shortstr]]
    :secure [[:challenge longstr]]
    :secure-ok [[:response longstr]]
    :tune [[:channel-max short] [:frame-max long] [:heartbeat short]]
    :tune-ok [[:channel-max short] [:frame-max long] [:heartbeat short]]
    :open [[:virtual-host path] [:reversed-1 any] [:reserved-2 any]]
    :open-ok [[:reserved-1 any]]
    :close [[:reply-code reply-code] [:reply-text reply-text] [:class-id class-id] [:method-id method-id]]
    :close-ok []}})

(def amqp-method-fields
  (into {} (for [[class-id class] amqp-classes [method-id method] (amqp-methods class)]
             [[class-id method-id] (map first (get-in amqp-method-signatures [class method]))])))

(def amqp-method-codecs
  (into {} (for [[class-id class] amqp-classes [method-id method] (amqp-methods class)]
             [[class-id method-id] (compile-frame (map second (get-in amqp-method-signatures [class method])))])))

(defcodec amqp-header [(string :ascii :length 4) octet octet octet octet])
(defcodec payload (repeated :byte :prefix long))
(defcodec amqp-frame [type channel payload frame-end])
(defcodec rest (repeated :byte :prefix :none))

(defmulti decode-payload (comp amqp-frame-types first))

(defmethod decode-payload :default [[frame-type channel payload]]
  (throw (ex-info "Unimplemented frame type" {:frame-type (get amqp-frame-types frame-type frame-type)
                                              :channel channel
                                              :payload payload})))

(defmethod decode-payload :method [[_ channel payload]]
  (let [[class-id method-id arguments] (decode [class-id method-id rest] (byte-array payload))
        class (amqp-classes class-id)
        method [class-id method-id]]
    {:type :method
     :channel channel
     :class class
     :method (get-in amqp-methods [class method-id])
     :payload (zipmap
               (amqp-method-fields method)
               (decode (amqp-method-codecs method) (byte-array arguments)))}))

(defmethod decode-payload :heartbeat [_] {:type :heatbeat})

(defn validate-frame [[type channel payload frame-end]]
  (if (= 0xCE (bit-and frame-end 0xFF))
    [type channel payload]
    (throw (ex-info "Invalid frame-end byte encountered" {:frame-end frame-end}))))

(defmulti encode-payload :type)

(defmethod encode-payload :method [{:keys [channel class method payload]}]
  (let [id [(amqp-classes-reverse class) (get-in amqp-methods-reverse [class method])]]
    (encode amqp-frame [1 channel
                        (decode rest
                                (concat (encode [class-id method-id] id)
                                        (encode (amqp-method-codecs id)
                                                (map payload (amqp-method-fields id)))))
                        (.byteValue 0xCE)])))

(defmulti postdecode (fn [{:keys [class method]}] [class method]))

(defmethod postdecode :default [x] x)

(let [null-regex (re-pattern (str (char 0)))]
  (defmethod postdecode [:connection :start-ok] [frame]
    (update-in frame [:payload :response]
               #(zipmap [:authcid :authzid :passwd] (str/split % null-regex)))))

(defn decode-amqp-frame [frame]
  (postdecode (decode-payload (validate-frame frame))))
