(ns rethink.core
  (:require [rethinkdb.query :as r]))

(def conn (r/connect :host "localhost"
                     :port 28015
                     :db "test"))

(defn create-authors-table
  []
  (-> (r/table-create "authors")
      (r/run conn)))

(def authors
  (r/table "authors"))

(def data
  [{:name "William Adama"
    :tv_show "Battlestar Galactica"
    :posts [{:title "Decommissioning Speech"
             :content "The Cylon War is long over..."}
            {:title "We are at war"
             :content "Moments ago, this ship received..."}
            {:title "The new Earth"
             :content "The discoveries of the past few days..."}]}
   {:name "Laura Roslin"
    :tv_show "Battlestar Galactica"
    :posts [{:title "The oath of office"
             :content "I, Laura Roslin, ..."}
            {:title "They look like us"
             :content "The Cylons have the ability..."}]}
   {:name "Jean-Luc Picard"
    :tv_show "Star Trek: TNG"
    :posts [{:title "Civil rings"
             :content "There are some words I've known since..."}]}])

(defn insert-author
  [data]
  (-> authors
      (r/insert data)
      (r/run conn)))

(defn get-authors
  []
  (-> authors
      (r/run conn)))

(defn get-authors-by-name
  [n]
  (-> authors
      (r/filter
       (r/fn [row] (r/eq (r/get-field row "name") n)))
      (r/run conn)))

(defn get-authors-with-more-than-n-posts
  [n]
  (-> authors
      (r/filter
       (r/fn [row] (r/gt (r/count (r/get-field row "posts")) n)))
      (r/run conn)))

(defn get-author-changes
  []
  (let [changes (-> (r/db "test")
                    (r/table "authors")
                    (r/changes {:include-initial true})
                    (r/run conn))]))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
