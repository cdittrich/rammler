(ns rammler.core
  (:require [rammler.amqp :as amqp]
            [aleph.tcp :as tcp]
            [gloss.io :refer [decode-stream decode encode]]
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

(def rabbit1-server "localhost")
(def rabbit1-port 5673)

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
(def platform (format "Clojure %s on %s %s" (clojure-version) (System/getProperty "java.vm.name") (System/getProperty "java.version")))
(def product "rammler")
(def version (get-version "lshift-de" "rammler"))

(defn decode-amqp-stream [src] (decode-stream src amqp/amqp-frame))

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


(defmulti handle-method-frame (fn [stream info {:keys [class method]}] [class method]))

(defmethod handle-method-frame :default [stream info frame]
  (println "Unhandled method frame" frame))

(defmethod handle-method-frame [:connection :start-ok] [stream info {:keys [payload] :as frame}]
  (let [{:keys [client-properties mechanism response]} payload
        {:keys [remote-addr server-port server-name]} info]
    (println (format "New Connection from %s (%s %s) -> %s:%d"
                     remote-addr (client-properties "product") (client-properties "version")
                     server-name server-port))
    (d/let-flow [conn (tcp/client {:host rabbit1-server :port rabbit1-port})]
       (d/chain (s/put! conn (encode amqp/amqp-header ["AMQP" 0 0 9 1]))
                (fn [_] (s/take! conn))
                (partial decode amqp/amqp-frame) amqp/decode-amqp-frame
                (fn [frame]
                  (println "Connected to RabbitMQ")
                  (s/connect stream conn)
                  (s/connect conn stream)
                  
                  (let [lock :bla]
                    (s/consume (fn [x] (locking lock (println "client" (amqp/decode-amqp-frame x)))) (decode-amqp-stream stream))
                    (s/consume (fn [x] (locking lock (println "rabbit" (amqp/decode-amqp-frame x)))) (decode-amqp-stream conn))))))))

(defn handler
  "Handle incoming AMQP 0.9.1 connections

  From the specification:
  2.2.4 The Connection Class
  AMQP is a connected protocol. The connection is designed to be long-lasting, and can carry multiple
  channels. The connection life-cycle is this:
  - The client opens a TCP/IP connection to the server and sends a protocol header. This is the only data
  - the client sends that is not formatted as a method.
  - The server responds with its protocol version and other properties, including a list of the security
    mechanisms that it supports (the Start method).
  - The client selects a security mechanism (Start-Ok).
  - The server starts the authentication process, which uses the SASL challenge-response model. It sends
    the client a challenge (Secure).
  - The client sends an authentication response (Secure-Ok). For example using the \"plain\" mechanism,
    the response consist of a login name and password.
  - The server repeats the challenge (Secure) or moves to negotiation, sending a set of parameters such as
    maximum frame size (Tune).
  - The client accepts or lowers these parameters (Tune-Ok).
  - The client formally opens the connection and selects a virtual host (Open).
  - The server confirms that the virtual host is a valid choice (Open-Ok).
  - The client now uses the connection as desired.
  - One peer (client or server) ends the connection (Close).
  - The other peer hand-shakes the connection end (Close-Ok).
  - The server and the client close their socket connection.

  There is no hand-shaking for errors on connections that are not fully open. Following successful protocol
  header negotiation, which is defined in detail later, and prior to sending or receiving Open or Open-Ok, a
  peer that detects an error MUST close the socket without sending any further data."
  [s info]
  (-> (s/take! s)
      (d/chain (partial decode amqp/amqp-header)
               (fn [header]
                 (println "Got header" header)
                 (if (= header ["AMQP" 0 0 9 1])
                   (let [s' (decode-amqp-stream s)
                         s'' (s/stream)]
                     (s/connect s s'')
                     (d/chain (send-start s) (fn [_] (s/take! s')) amqp/decode-amqp-frame
                              (fn [{:keys [type] :as frame}]
                                #_(s/consume-async (fn [x] (println "alternative" (map int x)) (d/deferred true)) s')
                                (handle-method-frame (s/splice s s'') info frame)
                                (prn frame)
                                
                                #_(d/loop [exit false]
                                    (when-not exit
                                      (d/chain (s/take! s') amqp/decode-amqp-frame
                                               (fn [{:keys [type] :as frame}]
                                                 (prn frame)
                                                 #_(handle-method-frame s'' info frame)
                                                 (d/recur (if (= type :method) (handle-method-frame s'' info frame) false))))))))))))
      (d/catch (fn [e] (println "Exception" e) (s/close! s)))))


#_(d/let-flow [conn (tcp/client {:host rabbit1-server :port rabbit1-port})]
    (s/connect s conn)
    (s/connect conn s)
    (s/consume (fn [x] (println "client" x)) (decode-amqp-stream s))
    (s/consume (fn [x] (println "rabbit" x)) (decode-amqp-stream conn)))

(defonce server (atom nil))

(defn start-server []
  (when @server (.close @server))
  (reset! server (tcp/start-server #'handler {:port 5672})))

(defn -main [& args]
  (start-server))

(comment
  (->> (byte-array payload) (gloss.io/decode amqp/amqp-frame) amqp/validate-frame amqp/decode-payload))
