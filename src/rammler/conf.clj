(ns rammler.conf
  (:require [trptcolin.versioneer.core :refer [get-version]]
            [config.core :refer [env]]
            [taoensso.timbre :as timbre]
            [clojure.string :as str]
            [clojure.java.io :as io]))

(def rabbit1-server "rabbitmq")
(def rabbit1-port 5672)

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
  (let [dir (env :log-directory "")]
    (when-not (str/blank? dir)
      (if (.canWrite (io/file dir))
        (timbre/merge-config!
         {:appenders
          {:spit (assoc (timbre/spit-appender {:fname (format "%s/%s" dir "rammler.log")})
                        :min-level (env :log-level))}})
        (throw (ex-info dir {:cause :log-unwritable}))))))
