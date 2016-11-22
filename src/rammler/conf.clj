;;;; rammler AMQP proxy
;;;; Copyright (C) 2016  LShift Services GmbH
;;;; 
;;;; This program is free software: you can redistribute it and/or modify
;;;; it under the terms of the GNU Affero General Public License as
;;;; published by the Free Software Foundation, either version 3 of the
;;;; License, or (at your option) any later version.
;;;; 
;;;; This program is distributed in the hope that it will be useful,
;;;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;;;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;;; GNU Affero General Public License for more details.
;;;; 
;;;; You should have received a copy of the GNU Affero General Public License
;;;; along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns rammler.conf
  (:require [trptcolin.versioneer.core :refer [get-version]]
            [taoensso.timbre :as timbre]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [rammler.util :as util]
            [clojure.spec :as s]))

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
(def hostname (.getHostName (java.net.InetAddress/getLocalHost)))

;; spec

(defn- writable-directory?
  "Does `s` correspond to a writable directory?"
  [s]
  (let [file (if (instance? java.io.File s) s (io/file s))]
    (if (and (.exists file) (.isDirectory file) (.canWrite file))
      file
      :clojure.spec/invalid)))

(defn- valid-interface?
  "Does `s` correspond to a valid interface?"
  [s]
  (if (instance? java.net.InetAddress s)
    s
    (try (util/inet-address s)
         (catch Exception _ :clojure.spec/invalid))))

(s/def ::base-config
  (s/keys
    :opt [:conf/log-level :conf/log-directory :conf/port :conf/ssl-port :conf/interface :conf/ssl-interface :conf/cluster-name :conf/trace? :conf/stats?]
    :req [:conf/strategy :conf/capabilities]))

(s/def :conf/strategy keyword?)

(defmulti strategy-type :conf/strategy)
(defmethod strategy-type :database [_]
  (s/merge ::base-config (s/keys :req [:conf/database])))
(defmethod strategy-type :static [_]
  (s/merge ::base-config (s/keys :req [:conf/static])))

(s/def ::config (s/multi-spec strategy-type :conf/strategy))

(s/def :conf/cluster-name string?)
(s/def :conf/log-level timbre/-levels-set)
(s/def :conf/log-directory (s/conformer writable-directory?))
(s/def :conf/port integer?)
(s/def :conf/ssl-port integer?)
(s/def :conf/interface (s/conformer valid-interface?))
(s/def :conf/ssl-interface (s/conformer valid-interface?))
(s/def :conf/trace? boolean?)
(s/def :conf/stats? boolean?)

(s/def :conf/database (s/keys :req [:database/spec :database/query]))
(s/def :conf/static (s/keys :req [:static/host :static/port] :opt [:static/throttle]))

(s/def :database/spec (s/keys :req-un [:database/subprotocol :database/subname :database/user :database/password]))
(s/def :database/query string?)
(s/def :database/subprotocol string?)
(s/def :database/subname string?)
(s/def :database/user string?)
(s/def :database/password string?)

(s/def :static/host string?)
(s/def :static/port integer?)
(s/def :static/throttle integer?)

(s/def :conf/capabilities (s/coll-of keyword?))

;; EDN reading

(defn- pushback-reader [o]
  (java.io.PushbackReader. o))

(defn- read-config [^java.io.File file]
  (cond (not (.exists file)) (throw (ex-info (str file) {:cause :no-configuration}))
        (not (.canRead file)) (throw (ex-info (str file) {:cause :unreadable-configuration}))
        (not (.isFile file)) (throw (ex-info (str file) {:cause :wrong-configuration-type}))
        :default (with-open [r (io/reader file)
                             pr (pushback-reader r)]
                   (try (edn/read pr)
                        (catch Exception e
                          (throw (ex-info (str file) {:cause :configuration-parse-error} e)))))))

(defn load-config
  ([s]
   (let [config (read-config s)
         config' (s/conform ::config config)]
     (if (= config' :clojure.spec/invalid)
       (throw (ex-info (s/explain-str ::config config) {:cause :configuration-error}))
       config')))
  ([] (load-config default-config)))

(defn process-config! [{:keys [conf/log-level conf/log-directory]}]
  (if log-level
    (timbre/merge-config!
      {:appenders {:println (assoc (timbre/println-appender {:stream :std-out})
                              :output-fn (comp force :msg_)
                              :min-level log-level)
                   :spit (if log-directory
                           (assoc (timbre/spit-appender {:fname (format "%s/%s" log-directory "rammler.log")})
                             :min-level log-level))}})
    (timbre/merge-config! {:enabled? false})))
