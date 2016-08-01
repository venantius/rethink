(ns rethink.ast.reql-ast-test
  (:require [clojure.test :refer :all]
            [rethink.ast.reql-ast :as ast]
            [rethink.java :as java]))

(deftest table-create-test
  (is (= (ast/table-create-test "authors")
         {:term-type :TABLE_CREATE
          :args ["authors"]
          :optargs nil})))
