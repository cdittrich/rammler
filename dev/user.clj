(ns user
  (:require [rammler.core :as core]
            [rammler.conf :as conf]
            [rammler.server :as server]
            [rammler.amqp :as amqp]
            [rammler.util :as util]

            [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.exchange  :as le]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]

            [taoensso.timbre :as timbre
             :refer (trace debug info warn error fatal spy with-log-level)]))

(conf/set-configuration!)

(timbre/merge-config!
 {:appenders
  {:println (assoc (timbre/println-appender {:stream :auto})
                   :output-fn (fn [{:keys [level msg_]}]
                                (format "%s> %s" (name level) (force msg_))))}})

(defn start []
  (server/start-server))


