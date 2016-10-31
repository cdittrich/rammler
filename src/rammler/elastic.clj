(ns rammler.elastic
  (:require [clojurewerkz.elastisch.rest          :as es]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.query         :as q]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojurewerkz.elastisch.rest.index    :as idx]
            [clojurewerkz.elastisch.rest.bulk     :as bulk]))

(def conn (es/connect "https://search-rammler-xuv735vsiomxcirhzuswfh4bdm.eu-central-1.es.amazonaws.com"
            {"cluster.name" "rammler"}))

(def mapping-types
  {"datum" {:properties {:timestamp {:type "date" :format "epoch_millis"}
                         :addr {:type "string"}
                         :type {:type "string"}}}})

(defn create-index! [conn index]
  (idx/create conn index :mappings mapping-types))

(defn delete-index! [conn index]
  (idx/delete conn index))

(defn register! [login doc]
  (when-not (idx/exists? conn login)
    (create-index! conn login))
  (esd/create conn login "datum" doc))

(defn bulk-register! [login docs]
  (when-not (idx/exists? conn login)
    (create-index! conn login))
  (bulk/bulk conn (bulk/bulk-create (map (partial merge {:_index login :_type "datum"}) docs))))
