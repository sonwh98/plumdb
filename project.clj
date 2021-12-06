(defproject plumbdb "0.1.0-SNAPSHOT"
  :description "Datalog implementation for Clojure and ClojureScript"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[com.taoensso/timbre "5.1.2"]
                 [mount "0.1.16"]
                 [org.clojure/clojure "1.10.3"]
                 [org.clojure/core.async "1.5.640"]]
  :repl-options {:init-ns plumdb.core}
  )
