(ns rethink.engine-test
  (:require [clojure.test :refer :all]
            [rethink.engine :as engine]))

(deftest reql-ast?-works
  (is (some? (engine/reql-ast?
              {:term-type :FILTER
               :args []
               :optargs nil}))))
