(ns rammler.elastic
  (:require [clojurewerkz.elastisch.rest          :as es]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.query         :as q]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojurewerkz.elastisch.rest.index    :as idx]))

(def conn (es/connect "https://search-rammler-xuv735vsiomxcirhzuswfh4bdm.eu-central-1.es.amazonaws.com"
            {"cluster.name" "rammler"}))

