(defproject lshift-de/rammler "0.1.0-SNAPSHOT"
  :description "RabbitMQ Proxy"
  :url "https://github.com/lshift-de/rammler"
  :license {:name "AGPLv3+"
            :url "http://www.gnu.org/licenses/agpl.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [aleph "0.4.1"]
                 [gloss "0.2.6"]
                 [trptcolin/versioneer "0.2.0"]
                 [camel-snake-kebab "0.4.0"]
                 [com.novemberain/langohr "3.6.1"]]
  :main ^:skip-aot rammler.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
