(ns rammler.conf
  (:require [trptcolin.versioneer.core :refer [get-version]]
            [taoensso.timbre :as timbre]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [rammler.util :as util]))

(def default-server-capabilities
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
(def default-config "/etc/rammler.edn")

(defn- writable-directory? [^java.io.File file]
  (or (and (.exists file) (.isDirectory file) (.canWrite file))
    (throw (ex-info (str file) {:cause :log-unwritable}))))

(def config-options
  {:log-level {:verificator timbre/-levels-set}
   :log-directory {:parser io/file :verificator writable-directory?}
   :port {:verificator integer?}
   :ssl-port {:verificator integer?}
   :interface {:parser util/inet-address}
   :ssl-interface {:parser util/inet-address}
   :strategy {:required true :verificator #{:database :static}}
   :database-url {:parser #(java.net.URL. %)}
   :database-query {:verificator string?}
   :static-host {:verificator string?}
   :static-port {:verificator integer?}
   :capabilities {:required true}})

(defn- pushback-reader [o]
  (java.io.PushbackReader. o))

(defn read-config
  ([^java.io.File file]
   (cond (not (.exists file)) (throw (ex-info (str file) {:cause :no-configuration}))
         (not (.canRead file)) (throw (ex-info (str file) {:cause :unreadable-configuration}))
         (not (.isFile file)) (throw (ex-info (str file) {:cause :wrong-configuration-type}))
         :default (with-open [r (io/reader file)
                              pr (pushback-reader r)]
                    (try (edn/read pr)
                         (catch Exception e
                           (throw (ex-info file {:cause :configuration-parse-error} e)))))))
  ([] (read-config (io/file default-config))))

(defn- check-unknown-keys [config]
  (let [unknown-keys (set/difference (set (keys config)) (set (keys config-options)))]
    (when (seq unknown-keys)
      (throw (ex-info (pr-str unknown-keys) {:cause :unknown-configuration-options})))))

(defn- check-missing-keys [config]
  (let [missing-keys (set/difference (set (keys (filter (comp :required second) config-options)))
                       (set (keys config)))]
    (when (seq missing-keys)
      (throw (ex-info (pr-str missing-keys) {:cause :missing-configuration-options})))))

(defn- throw-configuration-error [msg]
  (throw (ex-info msg {:cause :configuration-error})))

(defn- check-strategy [config]
  (case (config :strategy)
    :database (when-not (and (config :database-url) (config :database-query))
                (throw-configuration-error ":database strategy requires database-url and database-query to be set"))
    :static (when-not (and (config :static-host) (config :static-port))
              (throw-configuration-error ":static strategy requires static-host and static-port to be set")))
  config)

(defn load-config [config]
  (check-unknown-keys config)
  (check-missing-keys config)
  (check-strategy
    (into {}
      (filter identity
        (for [[option {:keys [parser verificator]
                       :or {parser identity verificator (constantly true)}}] config-options]
          (if-let [value (config option)]
            (let [value (parser value)]
              (if (not (verificator value))
                (throw-configuration-error (format "%s: %s" (name option) value))
                [option value]))))))))

(defn process-config! [{:keys [log-level log-directory]}]
  (if log-level
    (timbre/merge-config!
      {:appenders {:println (assoc (timbre/println-appender {:stream :std-out})
                              :output-fn (comp force :msg_)
                              :min-level log-level)
                   :spit (if log-directory
                           (assoc (timbre/spit-appender {:fname (format "%s/%s" log-directory "rammler.log")})
                             :min-level log-level))}})
    (timbre/merge-config! {:enabled? false})))
