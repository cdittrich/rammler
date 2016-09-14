(ns rammler.server
  (:require [rammler.amqp :as amqp]
            [rammler.util :as util]
            [rammler.conf :as conf]
            [aleph.tcp :as tcp]
            [aleph.netty :as netty]
            [gloss.io :refer [decode-stream decode encode]]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [camel-snake-kebab.core :refer :all]
            [camel-snake-kebab.extras :refer [transform-keys]]
            [taoensso.timbre :as timbre
             :refer (log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env log-env)]))

(defn send-start [stream]
  (s/put! stream (amqp/encode-frame {:type :method
                                     :channel 0
                                     :class :connection
                                     :method :start
                                     :payload {:version-major 0
                                               :version-minor 9
                                               :server-properties {"capabilities" (zipmap (map ->snake_case_string conf/server-capabilities) (repeat true))
                                                                   "cluster_name" "rabbit@broker1.adorno.in.lshift.de"
                                                                   "copyright" conf/copyright
                                                                   "information" conf/license
                                                                   "platform" conf/platform
                                                                   "product" conf/product
                                                                   "version" conf/version}
                                               :mechanisms "AMQPLAIN PLAIN"
                                               :locales "en_US"}})))

(defmulti handle-method-frame (fn [context {:keys [class method]}] [class method]))

(defmethod handle-method-frame :default [context frame]
  (error "Unhandled method frame" frame))

(defmethod handle-method-frame [:connection :start-ok] [{:keys [stream info resolver]} {:keys [payload] :as frame}]
  (let [{:keys [client-properties] {:keys [authzid]} :response} payload
        {:keys [remote-addr server-port server-name]} info]
    (println (format "New Connection from %s (%s %s) -> %s@%s:%d"
               remote-addr (client-properties "product") (client-properties "version")
               authzid server-name server-port))
    (debug "Client" frame)
    (if-let [server (resolver authzid)]
      (d/let-flow [conn (tcp/client server)]
       (d/chain (s/put! conn (encode amqp/amqp-header ["AMQP" 0 0 9 1]))
         (fn [_] (s/take! conn))
         (partial decode amqp/amqp-frame) amqp/decode-amqp-frame
         (fn [frame]
           (println "Connected to RabbitMQ")
           (debug "Server" (prn-str frame))
           (s/connect stream conn)
           (s/connect conn stream)
           (s/consume #(debug "Server" (pr-str %)) (amqp/decode-amqp-stream conn)))))
      (throw (ex-info "Unresolvable username" {:user authzid})))))

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
  [resolver]
  (fn [s info]
    (-> (s/take! s)
      (d/chain (partial decode amqp/amqp-header)
        (fn [header]
          (debug "Got header" header)
          (if (= header ["AMQP" 0 0 9 1])
            (let [s' (amqp/decode-amqp-stream s)
                  s'' (s/stream)]
              (s/connect s s'')
              (d/chain (send-start s) (fn [_] (s/take! s'))
                (fn [{:keys [type class method] :as frame}]
                  (s/consume #(debug "Client" (pr-str %)) s')
                  (if (= [type class method] [:method :connection :start-ok])
                    (handle-method-frame {:stream (s/splice s s'') :info info :resolver resolver} frame)
                    (throw (ex-info "Protocol violation" {:expected {:type :method :class :connection :method :start-ok}
                                                          :got (select-keys frame [type class method])})))))))))
      (d/catch (fn [e] (error "Exception" e) (s/close! s))))))

(defonce server (atom nil))
(defonce ssl-server (atom nil))

(defn start-server
  ([resolver {:keys [port ssl-port interface ssl-interface]}]
   (doseq [s [@server @ssl-server]]
     (try (when s (.close s)) (catch Exception _)))
   (let [f (handler resolver)
         listen? (and interface port)
         listen-ssl? (and ssl-interface ssl-port)]
     (when listen?
       (reset! server (tcp/start-server f {:socket-address (util/socket-address interface port)})))
     (when listen-ssl?
       (reset! ssl-server (tcp/start-server f {:socket-address (util/socket-address ssl-interface ssl-port) :ssl-context (netty/self-signed-ssl-context)})))
     (when-not (or listen? listen-ssl?)
       (throw (ex-info "Configured to not listen on any interfaces" {:cause :configuration-error})))
     (filter identity [(when listen? [interface port])
                       (when listen-ssl? [ssl-interface ssl-port])])))
  ([resolver] (start-server resolver {:port 5672 :ssl-port 5671 :interface "0.0.0.0" :ssl-interface "0.0.0.0"})))
