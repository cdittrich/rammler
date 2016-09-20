(ns rammler.core
  (:require [rammler.server :refer [start-server]]
            [rammler.conf :as conf]
            [aleph.netty :as netty]
            [guns.cli.optparse :refer (parse)]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str])
  (:import clojure.lang.ExceptionInfo)
  (:gen-class))

(taoensso.timbre/refer-timbre)

(def cli-options
  [["-h" "--help"        "Display this help and exit"]
   [nil  "--version"     "Output version information and exit"]
   ["-c" "--config FILE" "Location of configuration file" :default (io/file conf/default-config) :parse-fn io/file]])

(def agpl-notice "License AGPLv3+: GNU Affero General Public License version 3 or later <https://www.gnu.org/licenses/agpl-3.0.en.html>
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.")

(defn print-usage
  "Print usage to `*out*`"
  [banner]
  (println "Usage: rammler [options]")
  (println "RabbitMQ Proxy")
  (println banner))

(defn print-version
  "Print version information to `*out*`"
  []
  (println (format "rammler %s on %s %s"
                   conf/version
                   (System/getProperty "java.version")
                   (System/getProperty "java.vm.name")))
  (println conf/copyright)
  (println agpl-notice))

(defn parse-args
  "Optparse `args` and handle exceptions"
  [args]
  (try (parse args cli-options)
       (catch AssertionError e
         (throw (ex-info (.getMessage e) {:cause :cli-parser-error})))))

(defn strategy-resolver
  "Create resolver fn from `config`"
  [config]
  (let [{:keys [strategy database static]} config]
    (case strategy
      :database (fn [user] (first (jdbc/query (database :spec) (str/replace (database :query) "$user" user))))
      :static (constantly static))))

(defn run
  "Attempt to run rammler from `args`"
  [args]
  (let [[{:keys [help version config]} args banner] (parse-args args)]
    (cond help (do (print-usage banner) :exit)
          version (do (print-version) :exit)
          :default (let [config (if config (conf/load-config (io/file config)) (conf/load-config))
                         resolver (strategy-resolver config)]
                     (conf/process-config! config)
                     (let [interfaces (start-server resolver config)]
                       (infof "rammler (%s) running and listening on %s"
                         conf/version
                         (str/join ", " (map (partial str/join ":") interfaces))))))))

(defn handle-cause
  "Handle ExceptionInfo exceptions"
  [^ExceptionInfo e]
  (let [{:keys [cause]} (ex-data e)
        [msg code] (case cause
                     :no-configuration ["Configuration file doesn't exist" 1]
                     :unreadable-configuration ["Can't open configuration for reading" 1]
                     :wrong-configuration-type ["Configuration isn't a regular file" 1]
                     :configuration-parse-error ["Configuration parse error" 1]
                     :missing-configuration-option ["Option missing from configuration" 1]
                     :unknown-configuration-option ["Unknown configuration option" 1]
                     :configuration-error ["Configuration error" 2]
                     :cli-parser-error ["Command line error" 3]
                     :log-unwritable ["Can't write to log output directory" 4]
                     ["Unknown error" 255])]
    (fatal (format "%s: %s" msg (.getMessage e)))
    (trace (timbre/stacktrace e))
    code))

(defn -main
  "Set defaults and run rammler"
  [& args]
  (timbre/set-level! :trace)
  (timbre/merge-config!
   {:appenders {:println (assoc (timbre/println-appender {:stream :std-out})
                                :output-fn (comp force :msg_)
                                :min-level nil)}})
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread e]
        (errorf "Uncaught exception on %s: %s" (.getName thread) e)
        (trace (timbre/stacktrace e)))))
  (try
    (if (= (run args) :exit)
      (System/exit 0)
      ; In 0.4.2 (netty/wait-for-close)
      @(promise)) ; Workaround
    (catch ExceptionInfo e
      (System/exit (handle-cause e)))
    (catch Exception e
      (fatalf "rammler died with an unexpected error: %s" (.getMessage e))
      (trace (timbre/stacktrace e))
      (System/exit 255))))
