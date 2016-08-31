(ns rammler.conf
  (:require [trptcolin.versioneer.core :refer [get-version]]
            [config.core :refer [env]]
            [taoensso.timbre :as timbre]
            [clojure.string :as str]))

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
(def license "Licensed under the AGPLv3+.  See http://bigwig.io/")
(def platform (format "Clojure %s on %s %s" (clojure-version) (System/getProperty "java.vm.name") (System/getProperty "java.version")))
(def product "rammler")
(def version (get-version "lshift-de" "rammler"))

(defn set-configuration!
  "Set the project configuration based on the environment"
  []
  (timbre/set-level! (env :log-level))
  (let [dir (env :log-directory "")]
    (when-not (str/blank? dir)
      (timbre/merge-config!
       {:appenders
        {:spit (timbre/spit-appender {:fname (format "%s/%s" dir "rammler.log")})}}))))
