(ns rammler.core
  (:require [aleph.tcp :as tcp]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [gloss.core :as gloss :refer [defcodec]]
            [gloss.io :refer [decode encode decode-stream]]

            [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.exchange  :as le]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb])
  (:gen-class))

;;; Grammar
(defcodec short :uint16)
(defcodec long :uint32)
(defcodec octet :byte)
(defcodec shortstr (gloss/finite-frame octet (gloss/string :ascii)))
(defcodec longstr (gloss/finite-frame long (gloss/string :ascii)))
(defcodec value-type octet)

(defcodec boolean octet)
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
 ; Solve circular dependency table-value-types -> field-table -> field-value-pair -> field-value -> table-value-types
(declare table-value-types)
(defcodec field-value (gloss/header octet (comp table-value-types char) (fn [_] (throw (ex-info "Not implemented" {})))))
(defcodec field-array (gloss/repeated field-value :prefix long-int))
(defcodec field-name shortstr)
(defcodec field-value-pair [field-name field-value])
(defcodec field-table (gloss/finite-frame long-uint (gloss/repeated field-value-pair :prefix :none)))

(alter-var-root #'table-value-types
  (constantly
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
    \V :none}))

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

;;; Protocol
(defcodec amqp-header [(gloss/string :ascii :length 4) octet octet octet octet])
(defcodec amqp-frame [octet short (gloss/repeated octet :prefix long) octet])
(defcodec amqp-method-prefix [short short])

(def payload [1 0 0 0 0 1 -12 0 10 0 10 0 9 0 0 1 -49 12 99 97 112 97 98 105 108 105 116 105 101 115 70 0 0 0 -57 18 112 117 98 108 105 115 104 101 114 95 99 111 110 102 105 114 109 115 116 1 26 101 120 99 104 97 110 103 101 95 101 120 99 104 97 110 103 101 95 98 105 110 100 105 110 103 115 116 1 10 98 97 115 105 99 46 110 97 99 107 116 1 22 99 111 110 115 117 109 101 114 95 99 97 110 99 101 108 95 110 111 116 105 102 121 116 1 18 99 111 110 110 101 99 116 105 111 110 46 98 108 111 99 107 101 100 116 1 19 99 111 110 115 117 109 101 114 95 112 114 105 111 114 105 116 105 101 115 116 1 28 97 117 116 104 101 110 116 105 99 97 116 105 111 110 95 102 97 105 108 117 114 101 95 99 108 111 115 101 116 1 16 112 101 114 95 99 111 110 115 117 109 101 114 95 113 111 115 116 1 15 100 105 114 101 99 116 95 114 101 112 108 121 95 116 111 116 1 12 99 108 117 115 116 101 114 95 110 97 109 101 83 0 0 0 34 114 97 98 98 105 116 64 98 114 111 107 101 114 49 46 97 100 111 114 110 111 46 105 110 46 108 115 104 105 102 116 46 100 101 9 99 111 112 121 114 105 103 104 116 83 0 0 0 46 67 111 112 121 114 105 103 104 116 32 40 67 41 32 50 48 48 55 45 50 48 49 54 32 80 105 118 111 116 97 108 32 83 111 102 116 119 97 114 101 44 32 73 110 99 46 11 105 110 102 111 114 109 97 116 105 111 110 83 0 0 0 53 76 105 99 101 110 115 101 100 32 117 110 100 101 114 32 116 104 101 32 77 80 76 46 32 32 83 101 101 32 104 116 116 112 58 47 47 119 119 119 46 114 97 98 98 105 116 109 113 46 99 111 109 47 8 112 108 97 116 102 111 114 109 83 0 0 0 10 69 114 108 97 110 103 47 79 84 80 7 112 114 111 100 117 99 116 83 0 0 0 8 82 97 98 98 105 116 77 81 7 118 101 114 115 105 111 110 83 0 0 0 5 51 46 54 46 53 0 0 0 14 65 77 81 80 76 65 73 78 32 80 76 65 73 78 0 0 0 5 101 110 95 85 83 -50])

(def amqp-frame-types
  {1 :method
   2 :header
   3 :body
   4 :heartbeat})

(def amqp-classes
  {10 :connection
   20 :channel
   40 :exchange
   50 :queue
   60 :basic
   90 :tx})

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
    :close [[reply-code reply-code] [reply-text reply-text] [class-id class-id] [method-id method-id]]
    :close-ok []}})

(defn validate-frame [[type channel payload frame-end]]
  (if (= 0xCE (bit-and frame-end 0xFF))
    [type channel payload]
    (throw (ex-info "Invalid frame-end byte encountered" {:frame-end frame-end}))))

(defmulti decode-frame (comp amqp-frame-types first))

(defmethod decode-frame :method [[_ channel payload]]
  (let [[prefix arguments] (split-at 4 payload)
        [class-id method-id] (decode amqp-method-prefix (byte-array prefix))
        class (amqp-classes class-id)
        method (get-in amqp-methods [class method-id])
        signature (get-in amqp-method-signatures [class method])
        keys (map first signature)
        codec (map second signature)]
    (decode codec (byte-array arguments))))

(defn spy [prefix]
  (fn [x]
    (println (format "%s: %s" prefix x #_(pr-str (map int x)) #_(String. x) #_(decode amqp-header x)))
    x))

#_(defn handler [s info]
  (println "Got new connection in" info)
  (let [c @(tcp/client {:host "localhost" :port 5673})]
    (s/connect s c)
    (s/connect c s)))

#_(defn handler [s info]
  (println info)
  (d/let-flow [c (tcp/client {:host "localhost" :port 5673})]
    (s/connect (s/map (spy "server") c) s)
    (s/connect (s/map (spy "client") s) c)))

(defn handler [s info]
  (let [s' (decode-stream s amqp-header)]
    (d/chain (s/take! s')
      (fn [header]
        (s/close! s')
        (println "Got header" header)
        (if (= header ["AMQP" 0 0 9 1])
          (let [s' (decode-stream s amqp-frame)]
            (d/chain (s/put! s (byte-array payload))
                     (fn [_] (s/take! s'))
                     println))
          (s/close! s)))))

  #_(println @(s/take! (gloss.io/decode-stream s amqp)))
  
  
  #_(let [s' (gloss.io/decode-stream s :byte)]
    (d/loop [i (range 8)]
      (println @(s/take! s'))))
#_  (s/connect-via (spy "from client") s)
  #_(-> (s/put! s (byte-array payload))
      (d/chain
       #(fn [msg]
          (println msg)
          msg)
       (d/catch (fn [e]
                  (println e)
                  (s/close! s))))))

(defonce server (atom nil))

(defn start-server []
  (when @server (.close @server))
  (reset! server (tcp/start-server #'handler {:port 5672})))

(defn -main [& args]
  (start-server))

