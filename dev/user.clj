(ns user
  (:require [rammler.core :as core]
            [rammler.conf :as conf]
            [rammler.server :as server]
            [rammler.amqp :as amqp]
            [rammler.util :as util]
            [rammler.stats :as stats]

            [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.exchange  :as le]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]

            [clojure.java.jdbc :as jdbc]

            [clojure.string :as str]))

(taoensso.timbre/refer-timbre)

(timbre/merge-config!
 {:appenders
  {:println (assoc (timbre/println-appender {:stream :auto})
                   :min-level nil
                   :output-fn (fn [{:keys [level msg_]}]
                                (format "%s> %s" (name level) (force msg_))))}})

(timbre/set-level! :trace)

(def db {:dbtype "derby"
         :dbname "rammler"
         :create true})

(def query "select host, port, ssl from hosts join users on hosts.id = users.hostid where users.name = '$user'")

(defn start []
  (server/start-server (constantly {:host "localhost" :port 5673 :ssl false}) {:conf/stats? true :conf/trace? true}))

(defn reset-db []
  (jdbc/db-do-commands db
    [(jdbc/drop-table-ddl :users)
     (jdbc/drop-table-ddl :hosts)]))

(defn setup-db []
  (reset-db)
  (jdbc/db-do-commands db
    [(jdbc/create-table-ddl :users
       [[:name "VARCHAR(64)" "PRIMARY KEY"]
        [:hostid :int]])
     (jdbc/create-table-ddl :hosts
       [[:id :int "NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY"]
        [:host "VARCHAR(64)"]
        [:port :int]
        [:ssl "BOOLEAN" "DEFAULT FALSE"]])])

  (let [[{id1 :1} {id2 :1}]
        (jdbc/insert-multi! db :hosts
          [{:host "localhost" :port 5673 :ssl false}
           {:host "localhost" :port 5674 :ssl false}])]
    (jdbc/insert-multi! db :users
      [{:name "user1" :hostid id2}
       {:name "user2" :hostid id1}])))

(defn start-derby []
  (setup-db)
  (jdbc/with-db-connection [conn db]
    (server/start-server (fn [user]
                           (first
                             (jdbc/query db (str/replace query "$user" user)))))))

(comment
  (do (def conn (rmq/connect))
      (def ch (lch/open conn))
      (le/declare ch "out" "fanout" {:auto-delete true})
      (lq/declare ch "test-1" {:auto-delete true})
      (lq/declare ch "test-2" {:auto-delete true})
      (lq/bind ch "test-1" "out")
      (lq/bind ch "test-2" "out")
      (lb/publish ch "out" "" "Hello!" {:content-type "text/plain"})
      (lc/subscribe ch "test-1" (fn [& _]))
      (lc/subscribe ch "test-2" (fn [& _]))))
