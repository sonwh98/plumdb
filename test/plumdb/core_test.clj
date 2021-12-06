(ns plumdb.core-test
  (:require [clojure.test :refer :all]
            [mount.core :as mount]
            [plumdb.core :as db]))

(deftest plumdb-tests

  (testing "list-form->map-form"
    (let [query-as-map '{:find [?id ?name],
                         :in [$ $1 $2 $3],
                         :where
                         [[?id :email "son.c.to@gmail.com"]
                          [?id :email "foo@gmail.com"]
                          [?id :age ?age]]}
          query-as-list '[:find ?id ?name
                          :in $ $1 $2 $3
                          :where [?id :email "son.c.to@gmail.com"]
                          [?id :email "foo@gmail.com"]
                          [?id :age ?age]]]
      (is (= query-as-map (db/list-form->map-form query-as-list)))))
  
  (testing "testing q"
    (let [db  (atom {})
          datoms [[1 :person/first-name "sun"]
                  [1 :person/last-name "tzu"]
                  [1 :person/age 100]
                  [1 :person/description "author of the art of war"]
                  [1 :wikipedia/url "https://en.wikipedia.org/wiki/Sun_Tzu"]

                  [2 :person/first-name "sonny"]
                  [2 :person/last-name "to"]
                  [2 :person/age 1]
                  [2 :person/description "LISP enthusiast"]
                  [2 :wikipedia/url "https://en.wikipedia.org/wiki/Lisp"]
                  
                  [3 :person/first-name "Ben"]
                  [3 :person/last-name "Franklin"]
                  [3 :person/age 30]
                  [3 :person/description "a true renaissance man. scientist, engineer, inventor, politcian. author of poor richard's almaniac"]
                  [3 :wikipedia/url "https://en.wikipedia.org/wiki/Benjamin_Franklin"]

                  [4 :person/first-name "Isaac"]
                  [4 :person/last-name "Newton"]
                  [4 :person/age 50]
                  [4 :person/description "a true genius. scientist, engineer, inventor, alchemist, religious"]
                  [4 :wikipedia/url "https://en.wikipedia.org/wiki/Isaac_Newton"]

                  [5 :person/first-name "albert"]
                  [5 :person/last-name "einstein"]
                  [5 :person/age 39]
                  [5 :person/description "a genius. scientist. toppled newton's physics with a new theory"]
                  [5 :wikipedia/url "https://en.wikipedia.org/wiki/Albert_Einstein"]
                  ]
          db (db/index db datoms)]

      (is (= [[2 "sonny" "https://en.wikipedia.org/wiki/Lisp"]]
             (db/q '[:find ?id ?first-name ?url
                     :where
                     [?id :person/age 1]
                     [?id :person/first-name ?first-name]
                     [?id :wikipedia/url ?url]] @db)))

      (is (= [[1 "sun" "tzu"]
              [2 "sonny" "to"]
              [3 "Ben" "Franklin"]
              [4 "Isaac" "Newton"]
              [5 "albert" "einstein"]]
             (sort-by first (db/q '[:find ?id ?first-name ?last-name
                                    :where
                                    [?id :person/first-name ?first-name]
                                    [?id :person/last-name ?last-name]] @db))))
      
      (is (= [[2 "sonny" "to"]
              [3 "Ben" "Franklin"]
              [5 "albert" "einstein"]]
             (sort-by first (db/q '[:find ?id ?first-name ?last-name
                                    :where
                                    [?id :person/first-name ?first-name]
                                    [?id :person/last-name ?last-name]
                                    [?id :person/age (fn [age]
                                                       (< age 40))]] @db))))

      (is (= [[2 "sonny" "to"]
              [3 "Ben" "Franklin"]
              [5 "albert" "einstein"]]
             (let [max-age 40]
               (sort-by first (db/q {:find '[?id ?first-name ?last-name]
                                     :where ['[?id :person/first-name ?first-name]
                                             '[?id :person/last-name ?last-name]
                                             ['?id :person/age (fn [age]
                                                                 (< age max-age))]]} @db)))))

      (is (= [[1 "sun"] [3 "Ben"]]
             (sort-by first (db/q '[:find ?id ?first-name :where
                                    [?id :person/description #"author"]
                                    [?id :person/first-name ?first-name]] @db))))

      (is (= [[3 "Franklin"] [4 "Newton"] [5 "einstein"]]
             (sort-by first (db/q '[:find ?id ?last-name :where
                                    [?id :person/description #"scientist"]
                                    [?id :person/last-name ?last-name]] @db))))
      ))

  (testing "entity"
    (let [db  (atom {})
          datoms [[1 :person/first-name "sun"]
                  [1 :person/last-name "tzu"]
                  [1 :person/age 100]
                  [1 :person/description "author of the art of war"]
                  [1 :wikipedia/url "https://en.wikipedia.org/wiki/Sun_Tzu"]]
          db (db/index db datoms)]
      (is (= {:id 1,
              :person/first-name "sun",
              :person/last-name "tzu",
              :person/age 100,
              :person/description "author of the art of war",
              :wikipedia/url "https://en.wikipedia.org/wiki/Sun_Tzu"}
             (db/entity @db 1 )))
      )
    )

  )
