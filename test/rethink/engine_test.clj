(ns rethink.engine-test
  (:require [clojure.test :refer :all]
            [rethink.engine :as engine]))

(deftest term-to-class-works
  (is (= (#'engine/term-to-class :DB_CREATE)
         com.rethinkdb.gen.ast.DbCreate))
  (is (= (#'engine/term-to-class :DIV)
         com.rethinkdb.gen.ast.Div)))

(deftest reql-ast?-works
  (is (some? (engine/reql-ast?
              {:term-type :FILTER
               :args []
               :optargs nil}))))
