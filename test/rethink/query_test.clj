(ns rethink.query-test
  (:require [clojure.test :refer :all]
            [rethink.query :as query]
            [rethink.java :as java]))

(def conn (java/connect {:db "rethink_test"}))

(defn drop-and-create-db-fixture
  [t]
  (try
    (query/run (query/db-drop "rethink_test") conn)
    (catch Exception e)
    (finally
      (query/run (query/db-create "rethink_test") conn)
      (query/run (query/table-create "posts") conn)))
  (t))

;; These fixtures, in turn, test db-drop and db-create
(use-fixtures
  :each
  drop-and-create-db-fixture)

;; If this test fails it's likely that almost everything else will fail.
(deftest db-list-works
  (is (some #{"test"} (query/run (query/db-list) conn))))

;; Manipulating tables

(deftest table-create
  (let [res (-> (query/table-create "authors")
                (query/run conn))]
    (is (= (-> res :config_changes first :new_val :name) "authors"))
    (is (= (:tables_created res) 1)))
  (let [res (-> (query/db "rethink_test")
                (query/table-create "comments")
                (query/run conn))]
    (is (= (-> res :config_changes first :new_val :name) "comments"))
    (is (= (:tables_created res) 1))))

;; Writing data

(deftest insert-data-test
  (let [res (-> (query/table "posts")
                (query/insert {:title "Lorem Ipsum"})
                (query/run conn))]
    (is (= (:inserted res) 1)))
  (let [res (-> (query/table "posts")
                (query/insert [{:title "First post!"}
                               {:title "Second post!"}])
                (query/run conn))]
    (is (= (:inserted res) 2))))
