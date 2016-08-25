(ns rammler.util)

(defn locking-print [& more]
  (locking ::print-lock (apply println more)))
