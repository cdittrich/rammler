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

(def version "0.1.0")
(def pre-release "-pre")
(def build (if-let [n (System/getenv "BUILD_NUMBER")] (str "+build." n) ""))

(defproject lshift-de/rammler (str version pre-release build)
  :description "RabbitMQ Proxy"
  :url "https://github.com/lshift-de/rammler"
  :license {:name "AGPLv3+"
            :url "http://www.gnu.org/licenses/agpl.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha13"]
                 [aleph "0.4.1"]
                 [gloss "0.2.6"]
                 [trptcolin/versioneer "0.2.0"]
                 [camel-snake-kebab "0.4.0"]
                 [guns.cli/optparse "1.1.2"]
                 [com.taoensso/timbre "4.7.4"]
                 [cheshire "5.6.3"]
                 [org.clojure/java.jdbc "0.6.2-alpha3"]
                 [clojurewerkz/elastisch "2.2.2"]
                 [clj-time "0.12.0"]
                 [jarohen/chime "0.1.9"]]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :main rammler.core}
             :dev  {:dependencies [[com.novemberain/langohr "3.6.1"]
                                   [org.apache.derby/derby "10.12.1.1"]]
                    :resource-paths ["dev" "config"]}
             :prod {}
             :postgresql {:dependencies [[org.postgresql/postgresql "9.4.1210"]]}})
