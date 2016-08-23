(ns rammler.core
  (:require [rammler.amqp :as amqp]
            [aleph.tcp :as tcp]
            [gloss.io :refer [decode-stream]]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [trptcolin.versioneer.core :refer [get-version]]
            [camel-snake-kebab.core :refer :all]
            [camel-snake-kebab.extras :refer [transform-keys]]
            
            [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.exchange  :as le]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb])
  (:gen-class))

(def server-capabilities
  [:publisher-confirms
   :per-consumer-qos
   :exchange-exchange-bindings
   :authentication-failure-close
   :connection.blocked
   :consumer-cancel-notify
   :basic.nack
   :direct-reply-to
   :consumer-priorities])

(def copyright "Copyright (C) 2016 LShift Services GmbH")
(def license "Licensed under the MPL.  See http://www.rabbitmq.com/")
(def platform (format "%s %s" (System/getProperty "java.vm.name") (System/getProperty "java.version")))
(def product "rammler")
(def version (get-version "lshift-de" "rammler"))

(comment
  ; RabbitMQ server reply
  {:type :method
   :channel 0
   :class :connection
   :method :start
   :payload {:version-major 0
             :version-minor 9
             :server-properties {"capabilities" {"publisher_confirms" 1
                                                 "per_consumer_qos" 1
                                                 "exchange_exchange_bindings" 1
                                                 "authentication_failure_close" 1
                                                 "connection.blocked" 1
                                                 "consumer_cancel_notify" 1
                                                 "basic.nack" 1
                                                 "direct_reply_to" 1
                                                 "consumer_priorities" 1}
                                 "cluster_name" "rabbit@broker1.adorno.in.lshift.de"
                                 "copyright" "Copyright (C) 2007-2016 Pivotal Software, Inc."
                                 "information" "Licensed under the MPL.  See http://www.rabbitmq.com/"
                                 "platform" "Erlang/OTP"
                                 "product" "RabbitMQ"
                                 "version" "3.6.5"}
             :mechanisms "AMQPLAIN PLAIN"
             :locales "en_US"}})

(defn send-start [stream]
  (s/put! stream (amqp/encode-payload {:type :method
                                       :channel 0
                                       :class :connection
                                       :method :start
                                       :payload {:version-major 0
                                                 :version-minor 9
                                                 :server-properties {"capabilities" (zipmap (map ->snake_case_string server-capabilities) (repeat true))
                                                                     "cluster_name" "rabbit@broker1.adorno.in.lshift.de"
                                                                     "copyright" copyright
                                                                     "information" license
                                                                     "platform" platform
                                                                     "product" product
                                                                     "version" version}
                                                 :mechanisms "AMQPLAIN PLAIN"
                                                 :locales "en_US"}})))

(defn handler [s info]
  (let [s' (decode-stream s amqp/amqp-header)]
    (d/chain (s/take! s')
      (fn [header]
        (s/close! s')
        (println "Got header" header)
        (if (= header ["AMQP" 0 0 9 1])
          (let [s' (decode-stream s amqp/amqp-frame)]
            (d/chain (send-start s)
                     (fn [_]
                       (d/loop []
                         (d/chain (s/take! s')
                                  (comp println amqp/postdecode amqp/decode-amqp-frame))))))
          (s/close! s))))))

(defonce server (atom nil))

(defn start-server []
  (when @server (.close @server))
  (reset! server (tcp/start-server #'handler {:port 5672})))

(defn -main [& args]
  (start-server))

(comment
  (->> (byte-array payload) (gloss.io/decode amqp/amqp-frame) amqp/validate-frame amqp/decode-payload))
