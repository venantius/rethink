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

(def advertisement-1
  {:title "Lorem ipsum"
   :content "I sat at the sea shore..."
   :id 1})

(def advertisement-2
  {:title "iPhone6"
   :content "Taken with an iPhone 6"
   :id 2})

(def advertisements
  [advertisement-1
   advertisement-2])

(defn drop-and-create-db-fixture
  [t]
  (try
    (query/run (query/db-drop "rethink_test") conn)
    (catch Exception e)
    (finally
      (query/run (query/db-create "rethink_test") conn)
      (query/run (query/table-create "posts") conn)
      (query/run (query/table-create "advertisements" {:primary_key "title"}) conn)
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

(deftest index-tests
  (testing "That we can can create an index"
    (let [res (-> (query/table "advertisements")
                  (query/index-create "id")
                  (query/run conn))]
      (is (= res {:created 1}))))
  (testing "That we can create a multi index with a function"
    (let [res (-> (query/table "advertisements")
                  (query/index-create
                   "thing"
                   (query/fn [row] [(query/g row "a") (query/g row "b")])
                   {:multi true})
                  (query/run conn))]
      (is (= res {:created 1}))))
  (testing "That the indexes were actually created and we can list them"
    (let [res (-> (query/table "advertisements")
                  (query/index-list)
                  (query/run conn))]
      (is (= res ["id" "thing"]))))
  (testing "That we can describe an index"
    (let [res (-> (query/table "advertisements")
                  (query/index-status "thing")
                  (query/run conn))
          {:keys [multi index ready]} (first res)]
      (is (= multi true))
      (is (= index "thing"))
      (is (= ready false))))
  (testing "Waiting for indexes to be ready"
    (let [res (-> (query/table "advertisements")
                  (query/index-wait "thing")
                  (query/run conn))
          {:keys [ready]} (first res)]
      (is (= ready true))))
  (testing "That we can re-name an index"
    (let [res (-> (query/table "advertisements")
                  (query/index-rename "thing" "thing2")
                  (query/run conn))]
      (is (= res {:renamed 1}))))
  (testing "That we can't overwrite an existing index"
    (try
      (-> (query/table "advertisements")
          (query/index-rename "thing2" "id")
          (query/run conn))
      (catch Exception e
        (is (= (class e) com.rethinkdb.gen.exc.ReqlOpFailedError)))))
  (testing "That we can overwrite an existing index when we want to."
    (let [res (-> (query/table "advertisements")
                  (query/index-rename "thing2" "id" {:overwrite true})
                  (query/run conn))]
      (is (= res {:renamed 1}))))
  (testing "That we can drop an index"
    (let [res (-> (query/table "advertisements")
                  (query/index-drop "id")
                  (query/run conn))]
      (is (= res {:dropped 1})))))

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

(deftest update-test
  (testing "basic field addition"
    (let [res (-> (query/table "advertisements")
                  (query/update {:timestamp "2016-08-18"})
                  (query/run conn))]
      (is (= (:replaced res) 2))))
  (testing "filtered update"
    (let [res (-> (query/table "advertisements")
                  (query/filter {:title "Lorem ipsum"})
                  (query/update {:rating 4})
                  (query/run conn))]
      (is (= (:replaced res) 1))))
  (testing "optargs work"
    (let [res (-> (query/table "advertisements")
                  (query/update {:timestamp "2016-08-18"} {:return_changes true})
                  (query/run conn))]
      (is (some? (:changes res)))
      (is (= (:unchanged res) 2))))
  (testing "update with fn works"
    (let [res (-> (query/table "advertisements")
                  (query/update (query/fn [row] {:cost "$50.00"}))
                  (query/run conn))]
      (is (= (:replaced res) 2))))
  (testing "actually verify changes"
    (let [res (-> (query/table "advertisements")
                  (query/run conn))]
      (is (= (set
              (map
               #(select-keys % [:id :title :content :rating :timestamp :cost])
               res))
             (set [{:title "Lorem ipsum"
                    :id 1
                    :rating 4
                    :timestamp "2016-08-18"
                    :content "I sat at the sea shore..."
                    :cost "$50.00"}
                   {:title "iPhone6"
                    :id 2
                    :content "Taken with an iPhone 6"
                    :timestamp "2016-08-18"
                    :cost "$50.00"}]))))))

(deftest replace-test
  (testing "actually replaces"
    (let [res (-> (query/table "advertisements")
                  (query/get "iPhone6")
                  (query/replace {:title "iPhone6"
                                  :content "Shot on iPhone 6"})
                  (query/run conn))]
      (is (= (:replaced res) 1))))
  (testing "takes optargs"
    (let [res (-> (query/table "advertisements")
                  (query/get "Lorem ipsum")
                  (query/replace
                   {:title "Lorem ipsum"
                    :content "Blah blah blah"}
                   {:return_changes true})
                  (query/run conn))]
      (is (= (:replaced res) 1))
      (is (some? (:changes res))))))

(deftest delete-test
  (testing "deleting a single document"
    (let [res (-> (query/table "advertisements")
                  (query/get "iPhone6")
                  (query/delete)
                  (query/run conn))]
      (is (= (:deleted res) 1))))
  (testing "deleting all documents, with optargs"
    (let [res  (-> (query/table "advertisements")
                   (query/delete {:return_changes true})
                   (query/run conn))]
      (is (= (:deleted res) 1))
      (is (some? (:changes res))))))

(deftest sync-test
  (-> (query/table "advertisements")
      (query/get "Lorem ipsum")
      (query/replace
       {:title "Lorem ipsum"
        :content "Blah blah blah"}
       {:durability "soft"})
      (query/run conn))
  (let [res (-> (query/table "advertisements")
                (query/sync)
                (query/run conn))]
    (is (= res {:synced 1}))))

;; Selecting data

(deftest table-test
  (let [res (-> (query/table "advertisements")
                (query/run conn))]
    (is (= (set res)
           (set advertisements)))))

(deftest get-test
  (let [res (-> (query/table "advertisements")
                (query/get "iPhone6")
                (query/run conn))]
    (is (= res advertisement-2))))

;; TODO: add tests for secondary index queries
(deftest get-all-test
  (let [res (-> (query/table "advertisements")
                (query/get-all "iPhone6" "Lorem ipsum")
                (query/run conn))]
    (is (= (set res)
           (set advertisements)))))

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

;; String manipulation

(deftest match-works
  (let [res (-> "name@domain.com"
                (query/match ".*@(.*)")
                (query/run conn))]
    (is (= res {:start 0
                :end 15
                :str "name@domain.com"
                :groups [{:end 15, :start 5, :str "domain.com"}]}))))

(deftest split-works
  (let [res (-> "foo  bar bax"
                (query/split)
                (query/run conn))]
    (is (= res ["foo" "bar" "bax"])))
  (let [res (-> "12,37,,22,"
                (query/split ",")
                (query/run conn))]
    (is (= res ["12", "37", "", "22", ""])))
  (let [res (-> "12,37,,22,"
                (query/split "," 3)
                (query/run conn))]
    (is (= res ["12", "37", "", "22,"])))
  (let [res (-> "foo  bar bax"
                (query/split nil 1)
                (query/run conn))]
    (is (= res ["foo" "bar bax"]))))

(deftest upcase-works
  (let [res (-> "Sentence about LaTeX."
                (query/upcase)
                (query/run conn))]
    (is (= res "SENTENCE ABOUT LATEX."))))

(deftest downcase-works
  (let [res (-> "Sentence about LaTeX."
                (query/downcase)
                (query/run conn))]
    (is (= res "sentence about latex."))))

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
    (is (>= res 5))
    (is (= (type res) java.lang.Long)))
  (let [res (-> (query/random 2 4 {:float true})
                (query/run conn))]
    (is (< res 4))
    (is (>= res 2))
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
