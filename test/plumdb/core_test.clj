(ns plumdb.core-test
  (:require [clojure.test :refer :all]
            [mount.core :as mount]
            [stigmergy.plumdb :as db]))

(deftest triple-store-tests

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
    (let [db  {}
          vecetors-or-maps [[1 :person/first-name "sun"]
                            [1 :person/last-name "tzu"]
                            [1 :person/age 100]
                            [1 :person/description "author of the art of war"]
                            [1 :wikipedia/url "https://en.wikipedia.org/wiki/Sun_Tzu"]

                            {:id 2
                             :person/first-name "sonny"
                             :person/last-name "to"
                             :person/age 1
                             :person/description "LISP enthusiast"
                             :wikipedia/url "https://en.wikipedia.org/wiki/Lisp"}
                            
                            [3 :person/first-name "Ben"]
                            [3 :person/last-name "Franklin"]
                            [3 :person/age 30]
                            [3 :person/description "a true renaissance man. scientist, engineer, inventor, politcian. author of poor richard's almaniac"]
                            [3 :wikipedia/url "https://en.wikipedia.org/wiki/Benjamin_Franklin"]

                            {:id 4
                             :person/first-name "Isaac"
                             :person/last-name "Newton"
                             :person/age 50
                             :person/description "a true genius. scientist, engineer, inventor, alchemist, religious"
                             :wikipedia/url "https://en.wikipedia.org/wiki/Isaac_Newton"}

                            [5 :person/first-name "albert"]
                            [5 :person/last-name "einstein"]
                            [5 :person/age 39]
                            [5 :person/description "a genius. scientist. toppled newton's physics with a new theory"]
                            [5 :wikipedia/url "https://en.wikipedia.org/wiki/Albert_Einstein"]
                            ]
          db (db/index db vecetors-or-maps)]

      (is (= [[2 "sonny" "https://en.wikipedia.org/wiki/Lisp"]]
             (db/q '[:find ?id ?first-name ?url
                     :where
                     [?id :person/age 1]
                     [?id :person/first-name ?first-name]
                     [?id :wikipedia/url ?url]] db)))

      (is (= [[1 "sun" "tzu"]
              [2 "sonny" "to"]
              [3 "Ben" "Franklin"]
              [4 "Isaac" "Newton"]
              [5 "albert" "einstein"]]
             (sort-by first (db/q '[:find ?id ?first-name ?last-name
                                    :where
                                    [?id :person/first-name ?first-name]
                                    [?id :person/last-name ?last-name]] db))))
      
      (is (= [[2 "sonny" "to"]
              [3 "Ben" "Franklin"]
              [5 "albert" "einstein"]]
             (sort-by first (db/q '[:find ?id ?first-name ?last-name
                                    :where
                                    [?id :person/first-name ?first-name]
                                    [?id :person/last-name ?last-name]
                                    [?id :person/age (fn [age]
                                                       (< age 40))]] db))))

      (is (= [[2 "sonny" "to"]
              [3 "Ben" "Franklin"]
              [5 "albert" "einstein"]]
             (let [max-age 40]
               (sort-by first (db/q {:find '[?id ?first-name ?last-name]
                                     :where ['[?id :person/first-name ?first-name]
                                             '[?id :person/last-name ?last-name]
                                             ['?id :person/age (fn [age]
                                                                 (< age max-age))]]} db)))))

      (is (= [[1 "sun"] [3 "Ben"]]
             (sort-by first (db/q '[:find ?id ?first-name :where
                                    [?id :person/description #"author"]
                                    [?id :person/first-name ?first-name]] db))))

      (is (= [[3 "Franklin"] [4 "Newton"] [5 "einstein"]]
             (sort-by first (db/q '[:find ?id ?last-name :where
                                    [?id :person/description #"scientist"]
                                    [?id :person/last-name ?last-name]] db))))
      ))

  (testing "entity"
    (let [db {}
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
             (db/entity db 1 )))
      )
    )

  #_(testing "macro recur-next-clauses"
      (let [repeated-code '(recur
                            (rest clauses)
                            (if (e-ids :init)
                              matching-ids
                              (clojure.set/intersection e-ids matching-ids))
                            bind-symbol->attr)]
        (is (= repeated-code (macroexpand '(db/recur-next-clauses))))))


  (testing "flatten-datoms"
    (let [datom-or-entity [[1 :name "foo"]
                           {:id 1
                            :age 1
                            :email "foo@bar.com"}]
          nested-datoms (map db/entity->datoms datom-or-entity)
          datoms (db/flatten-datoms nested-datoms)]
      (is (= '([1 :name "foo"] [[1 :age 1] [1 :email "foo@bar.com"]])
             nested-datoms))

      (is (= '([1 :name "foo"] [1 :age 1] [1 :email "foo@bar.com"])
             datoms)))    
    )

  )
