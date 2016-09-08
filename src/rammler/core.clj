(ns rammler.core
  (:require [rammler.server :refer [start-server]]
            [rammler.conf :as conf]
            [aleph.netty :as netty]
            [guns.cli.optparse :refer (parse)])
  (:import clojure.lang.ExceptionInfo)
  (:gen-class))

(taoensso.timbre/refer-timbre)

(def cli-options
  [["-h" "--help"    "Display this help and exit"]
   [nil  "--version" "Output version information and exit"]
   ["-q" "--quiet"   "Mute output"]
   ["-v" "--verbose" "Verbose output"]
   [nil  "--debug"   "Debugging output (takes precedence over --verbose)"]
   [nil  "--trace"   "Tracing output (takes precedence over --verbose and --debug)"]])

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

(defn handle-options
  "Handle side effects of command line `options`"
  [options]
  (if (options :quiet)
    (timbre/merge-config! {:appenders {:println {}}})
    (let [level (or (some (set (map first (filter second options))) [:trace :debug :verbose])
                    :info)]
      (timbre/merge-config!
       {:appenders {:println (assoc (timbre/println-appender {:stream :std-out})
                                    :output-fn (comp force :msg_)
                                    :min-level level)}}))))

(defn parse-args
  "Optparse `args` and handle exceptions"
  [args]
  (try (parse args cli-options)
       (catch AssertionError e
         (throw (ex-info (.getMessage e) {:cause :cli-parser-error})))))

(defn run
  "Attempt to run rammler from `args`"
  [args]
  (conf/set-configuration!)
  (let [[options args banner] (parse-args args)]
    (handle-options options)
    (or (cond (:help options) (print-usage banner)
              (:version options) (print-version)
              :default (start-server))
        0)))

(defn handle-cause
  "Handle ExceptionInfo exceptions"
  [^ExceptionInfo e]
  (let [{:keys [cause]} (ex-data e)
        [msg code] (case cause
                     :cli-parser-error ["Command line error" 5]
                     :log-unwritable ["Can't write to log output directory" 6]
                     ["Unknown error" 255])]
    (error (format "%s: %s" msg (.getMessage e)))
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
  (try
    (run args)
    ; In 0.4.2 (netty/wait-for-close)
    @(promise) ; Workaround
    (catch ExceptionInfo e
      (System/exit (handle-cause e)))
    (catch Exception e
      (error (format "rammler died with an unexpected error: %s" (.getMessage e)))
      (trace (timbre/stacktrace e))
      (System/exit 255))))
