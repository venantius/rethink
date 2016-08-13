(ns rethink.query-test
  (:require [clojure.test :refer :all]
            [rethink.query :as query]
            [rethink.java :as java]))

(def conn (java/connect {:db "rethink_test"}))

(defn insert-advertisements
  [data]
  (-> (query/db "rethink_test")
      (query/table "advertisements")
      (query/insert data)
      (query/run conn)))

(def advertisements
  [{:title "Lorem ipsum"
    :content "I sat at the sea shore..."}
   {:title "iPhone6"
    :content "Taken with an iPhone 6"}])

(defn drop-and-create-db-fixture
  [t]
  (try
    (query/run (query/db-drop "rethink_test") conn)
    (catch Exception e)
    (finally
      (query/run (query/db-create "rethink_test") conn)
      (query/run (query/table-create "posts") conn)
      (query/run (query/table-create "advertisements") conn)
      (insert-advertisements advertisements)))
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
  (testing "That we can create a basic table"
    (let [res (-> (query/table-create "authors")
                  (query/run conn))]
      (is (= (-> res :config_changes first :new_val :name) "authors"))
      (is (= (:tables_created res) 1))))
  (testing "That we can create a table when pased a db"
    (let [res (-> (query/db "rethink_test")
                  (query/table-create "comments")
                  (query/run conn))]
      (is (= (-> res :config_changes first :new_val :name) "comments"))
      (is (= (:tables_created res) 1))))
  (testing "That we can create a table with optional args specified"
    (let [res (-> (query/table-create "users" {:primary_key "name"})
                  (query/run conn))]
      (is (= (-> res :config_changes first :new_val :primary_key) "name")))))

;; Writing data

(deftest insert-test
  (let [res (-> (query/table "posts")
                (query/insert {:title "Lorem Ipsum"})
                (query/run conn))]
    (is (= (:inserted res) 1)))
  (let [res (-> (query/table "posts")
                (query/insert [{:title "First post!"}
                               {:title "Second post!"}])
                (query/run conn))]
    (is (= (:inserted res) 2))))

;; Selecting data

(deftest table-test
  (let [res (-> (query/table "advertisements")
                (query/run conn))]
    (= (map #(select-keys % [:title :content]) res)
       advertisements)))

(deftest filter-test
  (let [res (-> (query/table "advertisements")
                (query/filter {:title "Lorem ipsum"})
                (query/run conn))]
    (is (= "I sat at the sea shore..."
           (-> res first :content)))))

;; Document manipulation

(deftest get-field-works
  (testing "get-field works on a table"
    (let [res (-> (query/table "advertisements")
                  (query/get-field "title")
                  (query/run conn))]
      (is (= res
             (map :title advertisements))))))
