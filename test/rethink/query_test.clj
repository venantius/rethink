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
      (is (= (set res)
             (set (map :title advertisements)))))))

;; Math and logic

(deftest add-works
  (let [res (-> 2
                (query/add 3)
                (query/run conn))]
    (is (= res 5))))

(deftest sub-works
  (let [res (-> 5
                (query/sub 3)
                (query/run conn))]
    (is (= res 2))))

(deftest mul-works
  (testing "with numbers"
    (let [res (-> 5
                  (query/mul 3)
                  (query/run conn))]
      (is (= res 15))))
  (testing "with arrays"
    (let [data ["this" "is" "repeated"]
          res (-> (apply query/make-array data)
                  (query/mul 3)
                  (query/run conn))]
      (is (= res (into [] (flatten (repeat 3 data))))))))

(deftest div-works
  (let [res (-> 10
                (query/div 2)
                (query/run conn))]
    (is (= res 5))))

(deftest mod-works
  (let [res (-> 10
                (query/mod 3)
                (query/run conn))]
    (is (= res 1))))

(deftest and-works
  (let [res (-> true
                (query/and false)
                (query/run conn))]
    (is (= res false)))
  (let [res (-> true
                (query/and true)
                (query/run conn))]
    (is (= res true))))

(deftest or-works
  (let [res (-> true
                (query/or false)
                (query/run conn))]
    (is (= res true)))
  (let [res (-> false
                (query/or false)
                (query/run conn))]
    (is (= res false))))

(deftest ne-works
  (let [res (-> 1
                (query/ne 1 1)
                (query/run conn))]
    (is (= res false)))
  (let [res (-> 2
                (query/ne 1)
                (query/run conn))]
    (is (= res true))))

(deftest gt-works
  (let [res (-> 1
                (query/gt 2)
                (query/run conn))]
    (is (= res false)))
  (let [res (-> 2
                (query/gt 1)
                (query/run conn))]
    (is (= res true)))
  (let [res (-> (query/gt 3 2 1)
                (query/run conn))]
    (is (= res true)))
  (let [res (-> (query/gt 2 1 3)
                (query/run conn))]
    (is (= res false))))

(deftest ge-works
  (let [res (-> (query/ge 1 2)
                (query/run conn))]
    (is (= res false)))
  (let [res (-> (query/ge 1 1)
                (query/run conn))]
    (is (= res true)))
  (let [res (-> (query/ge 3 2 1)
                (query/run conn))]
    (is (= res true)))
  (let [res (-> (query/ge 2 1 3)
                (query/run conn))]
    (is (= res false))))

(deftest lt-works
  (let [res (-> (query/lt 1 2)
                (query/run conn))]
    (is (= res true)))
  (let [res (-> (query/lt 2 1)
                (query/run conn))]
    (is (= res false)))
  (let [res (-> (query/lt 1 2 3)
                (query/run conn))]
    (is (= res true)))
  (let [res (-> (query/lt 2 1 3)
                (query/run conn))]
    (is (= res false))))

(deftest le-works
  (let [res (-> (query/le 1 2)
                (query/run conn))]
    (is (= res true)))
  (let [res (-> (query/le 1 1)
                (query/run conn))]
    (is (= res true)))
  (let [res (-> (query/le 1 2 3)
                (query/run conn))]
    (is (= res true)))
  (let [res (-> (query/le 2 1 3)
                (query/run conn))]
    (is (= res false))))

(deftest not-works
  (let [res (-> (query/not true)
                (query/run conn))]
    (is (= res false))))

(deftest random-works
  (let [res (-> (query/random 5 10)
                (query/run conn))]
    (is (< res 10))
    (is (> res 5))
    (is (= (type res) java.lang.Long)))
  (let [res (-> (query/random 2 4 {:float true})
                (query/run conn))]
    (is (< res 4))
    (is (> res 2))
    (is (= (type res) java.lang.Double)))
  (let [res (-> (query/random)
                (query/run conn))]
    (is (< res 1))
    (is (> res 0))))

(deftest round-works
  (let [res (-> (query/round 1.1)
                (query/run conn))]
    (is (= res 1)))
  (let [res (-> (query/round 1.9)
                (query/run conn))]
    (is (= res 2))))

(deftest ceil-works
  (let [res (-> (query/ceil 1.1)
                (query/run conn))]
    (is (= res 2)))
  (let [res (-> (query/ceil 1.9)
                (query/run conn))]
    (is (= res 2))))

(deftest floor-works
  (let [res (-> (query/floor 1.1)
                (query/run conn))]
    (is (= res 1)))
  (let [res (-> (query/floor 1.9)
                (query/run conn))]
    (is (= res 1))))
