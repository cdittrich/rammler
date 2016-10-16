(ns rammler.elastic
  (:require [clojurewerkz.elastisch.rest          :as es]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.query         :as q]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojurewerkz.elastisch.rest.index    :as idx]))

(def conn (es/connect "https://search-rammler-xuv735vsiomxcirhzuswfh4bdm.eu-central-1.es.amazonaws.com"
            {"cluster.name" "rammler"}))

#_(def mapping-types
  {"datum" {:properties {:from {:type "date" :format "epoch_millis" :index "no"}
                         :to {:type "date" :format "epoch_millis" :index "no"}
                         :addr {:type "string" :index "not_analyzed"}
                         :type {:type "string" :index "not_analyzed"}
                         :count {:type "long" :index "no"}}}})

(def mapping-types
  {"datum" {:properties {:from {:type "date" :format "epoch_millis"}
                         :to {:type "date" :format "epoch_millis"}
                         :addr {:type "string"}
                         :type {:type "string"}
                         :count {:type "long"}}}})

(defn create-index! [conn index]
  (idx/create conn index :mappings mapping-types))

(defn delete-index! [conn index]
  (idx/delete conn index))

(defn register! [login doc]
  (when-not (idx/exists? conn login)
    (create-index! conn login))
  (esd/create conn login "datum" doc))
