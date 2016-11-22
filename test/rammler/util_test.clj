(ns rammler.util-test
  (:require [rammler.util :as util]
            [clojure.test :refer :all]))

(deftest ^{:plan 10} flagfn
  (testing "zero flags"
    (let [f (util/flagfn [])]
      (doseq [i (range 0 10)] (is (= #{} (f i)))))))

#_(let [powers-of-two (iterate (partial * 2) 1)]
  (defn flagfn
    "Return new `fn` that returns sets of `flags` for bitmasks"
    [flags]
    (let [bitmask (zipmap powers-of-two flags)]
      (fn [n]
        (->> (filter (complement #(zero? (bit-and (or n 0) %))) (keys bitmask))
             (select-keys bitmask) vals set)))))

#_(defn inet-address [s]
  (if (instance? java.net.InetAddress s)
    s
    (java.net.InetAddress/getByName s)))

#_(defn socket-address [address port]
  (java.net.InetSocketAddress. (inet-address address) port))
