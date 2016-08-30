(ns rammler.util)

(let [powers-of-two (iterate (partial * 2) 1)]
  (defn flagfn
    "Return new `fn` that returns sets of `flags` for bitmasks"
    [& flags]
    (let [bitmask (zipmap powers-of-two flags)]
      (fn [n]
        (->> (filter (complement #(zero? (bit-and n %))) (keys bitmask))
             (select-keys bitmask) vals set)))))
