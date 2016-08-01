(defproject rethink "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :javac-options  ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.apa512/rethinkdb "0.15.26"]
                 [cheshire "5.6.3"]
                 [com.rethinkdb/rethinkdb-driver "2.3.0"]])
