(ns rammler.amqp-test
  (:require [rammler.amqp :as amqp]
            [clojure.test :refer :all]))

(deftest ^{:plan 5} basic
  (testing "some basics"
    (is (map? amqp/spec))
    (is (= {10 :connection 20 :channel 30 :access 40 :exchange 50 :queue 60 :basic 90 :tx 85 :confirm}
          amqp/amqp-classes))
    (is (= {:connection 10 :channel 20 :access 30 :exchange 40 :queue 50 :basic 60 :tx 90 :confirm 85}
          amqp/amqp-classes-reverse))
    (is (= {:connection {20 :secure 60 :blocked 50 :close 21 :secure-ok 31 :tune-ok 40 :open 41 :open-ok 61 :unblocked 51 :close-ok 11 :start-ok 30 :tune 10 :start}
            :channel {10 :open 11 :open-ok 20 :flow 21 :flow-ok 40 :close 41 :close-ok}
            :access {10 :request 11 :request-ok}
            :exchange {10 :declare 11 :declare-ok 20 :delete 21 :delete-ok 30 :bind 31 :bind-ok 40 :unbind 51 :unbind-ok}
            :queue {20 :bind 50 :unbind 21 :bind-ok 31 :purge-ok 40 :delete 41 :delete-ok 51 :unbind-ok 11 :declare-ok 30 :purge 10 :declare}
            :basic {70 :get 110 :recover 20 :consume 72 :get-empty 60 :deliver 50 :return 21 :consume-ok 31 :cancel-ok 40 :publish 90 :reject 100 :recover-async 111 :recover-ok 11 :qos-ok 120 :nack 30 :cancel 10 :qos 71 :get-ok 80 :ack}
            :tx {10 :select 11 :select-ok 20 :commit 21 :commit-ok 30 :rollback 31 :rollback-ok} :confirm {10 :select 11 :select-ok}}
          amqp/amqp-methods))
    (is (= {:connection {:open 40 :start-ok 11 :tune 30 :unblocked 61 :start 10 :close 50 :open-ok 41 :tune-ok 31 :close-ok 51 :blocked 60 :secure 20 :secure-ok 21}
            :channel {:open 10 :open-ok 11 :flow 20 :flow-ok 21 :close 40 :close-ok 41}
            :access {:request 10 :request-ok 11}
            :exchange {:declare 10 :declare-ok 11 :delete 20 :delete-ok 21 :bind 30 :bind-ok 31 :unbind 40 :unbind-ok 51}
            :queue {:purge-ok 31 :declare 10 :delete-ok 41 :delete 40 :declare-ok 11 :bind-ok 21 :purge 30 :unbind 50 :unbind-ok 51 :bind 20}
            :basic {:deliver 60 :qos 10 :get 70 :qos-ok 11 :cancel-ok 31 :return 50 :recover-ok 111 :consume 20 :recover-async 100 :consume-ok 21 :ack 80 :nack 120 :cancel 30 :reject 90 :recover 110 :get-empty 72 :get-ok 71 :publish 40}
            :tx {:select 10 :select-ok 11 :commit 20 :commit-ok 21 :rollback 30 :rollback-ok 31} :confirm {:select 10 :select-ok 11}}
          amqp/amqp-methods-reverse))))
