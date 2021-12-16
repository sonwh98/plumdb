(ns plumdb.core
  (:require
   [clojure.core.async :as a :include-macros true]
   [clojure.java.io :as io]
   [clojure.pprint :as pp :refer :all]
   [mount.core :as mount :refer [defstate]]
   [taoensso.timbre :as log]))

(defn flush-val [a-val file]
  (when a-val
    (log/info "flush-val " file)
    (with-open [w (io/writer (io/file file))]
      (pp/pprint a-val w))))

(defn load-val [file]
  (try
    (let [val (-> (io/file file) slurp read-string)]
      (log/info "load-val " file)
      val)
    (catch Exception ex
      nil)))

(defstate tx-queue
  :start (a/chan 1024)
  :stop (a/close! tx-queue))

(defn- attr->kw [datoms-as-json]
  (mapv (fn [datom-as-json]
          (update datom-as-json 1 #(keyword %)))
        datoms-as-json))

(defn transact [tx-queue datoms-or-entities]
  (log/debug {:datoms-or-entities datoms-or-entities})
  (a/put! tx-queue datoms-or-entities))

(defn- initdb-from [file]
  (let [datoms (-> file io/file slurp read-string attr->kw)]
    (log/info "initdb-from " file)
    (transact tx-queue datoms)))

(defstate db
  :start (if (.exists (io/file "resources/db.edn"))
           (atom (load-val "resources/db.edn"))
           (do
             #_(initdb-from "resources/original-data.json")
             (atom {})))
  :stop (flush-val @db "resources/db.edn"))

(defn normalize-datoms
  "a datom is a tuple of 5 elements [e a v tx op]
  add tx and op positions if it is not already there in the datom"
  [datoms]
  (mapv (fn [[e a v tx op :as datom]]
          (cond-> datom
            (nil? tx) (assoc 3 (System/currentTimeMillis))
            (nil? op) (assoc 4 true)))
        datoms))

(defn entity->datoms
  "convert an entity map into a vector of datom triples [e a v]"
  [entity-map]
  (if-let [id (:id entity-map)]
    (vec (keep (fn [[a v]]
                 (when-not (= a :id)
                   [id a v]))
               entity-map))
    entity-map))

(defn flatten-datoms [nested-datoms]
  (reduce (fn [acc d]
            (if (vector? (first d))
              (concat acc d)
              (concat acc [d])))
          []
          nested-datoms))

(defn index
  "index the given datoms into the db.
  db is a map with keys :tx-log :eavt :avet :aevt
  datoms is a list/vector of datom"
  [db datoms-or-entities]
  (let [nested-datoms (map entity->datoms datoms-or-entities)
        datoms (-> nested-datoms flatten-datoms normalize-datoms)]
    (let [db (merge-with concat db {:tx-log datoms})
          db (merge-with (fn [& indexes]
                           (apply merge-with concat indexes))
                         db
                         {:eavt (group-by first datoms)}
                         {:avet (group-by second datoms)}
                         {:aevt (group-by #(nth % 2) datoms)})]
      db)))

(defn- start-transaction-indexer
  "index datoms from tx-queue"
  [stop-ch]
  (log/info "starting transaction indexer")
  (a/go-loop []
    (let [[datoms-or-entity-maps ch] (a/alts! [tx-queue stop-ch]
                                              :priority true)]
      (if (= ch tx-queue)
        (do
          (swap! db merge (index @db datoms-or-entity-maps))
          (recur))
        (log/info "stopping transaction indexer")))))

(defstate transactor
  :start (let [stop-ch (a/chan 1)]
           (start-transaction-indexer stop-ch)
           stop-ch)
  :stop (let [stop-ch transactor]
          (a/put! stop-ch true)))

(defn entity [db eid]
  (let [datoms-for-eid (get (:eavt db) eid)
        k-v-pairs (map (fn [datom]
                         (subvec datom 1 3))
                       datoms-for-eid)]
    (into {:id eid} k-v-pairs)))

(defn list-form->map-form
  "transform datalog query in list form to map form "
  [list-form]
  (let [where-i (.indexOf list-form :where)
        in-i (.indexOf list-form :in)
        end-i (count list-form)
        variables (subvec list-form 1 (if (pos? in-i)
                                        in-i
                                        where-i))
        inputs (if (pos? in-i)
                 (subvec list-form (inc in-i) where-i)
                 '[$])
        where-clauses  (subvec list-form (inc where-i) end-i)]
    {:find variables
     :in inputs
     :where where-clauses}))

(defn regex? [re]
  (= (type re)
     java.util.regex.Pattern))

(defn lambda?
  "predicate to determine whether s-expression can be evaluated as function"
  [s-expression]
  (or (fn? s-expression)
      (and (list? s-expression)
           (let [f (first s-expression)]
             (and (symbol? f)
                  (= (name f) "fn"))))))

(defmacro recur-next-clauses
  "macro to reduce boiler-plate code to recur in q function"
  []
  (let [e-ids# '(if (e-ids :init) 
                  matching-ids
                  (clojure.set/intersection e-ids matching-ids))]
    (concat '(recur (rest clauses))
            [e-ids#]
            '[bind-symbol->attr])))

(defn e-v-pairs [db a]
  "return e, v positions of datoms in the :avet index of db for given attribute a"
  [db a]
  (let [avet (:avet db)
        datoms (avet a)]
    (mapv (fn [[e a v tx op :as datom]]
            [e v])
          datoms)))

(defn q [datalog-query db]
  (let [query-map-form (cond
                         (sequential? datalog-query) (list-form->map-form datalog-query)
                         (map? datalog-query) datalog-query
                         :else (throw (ex-info "datalog-query must be a list/vector or map"
                                               {:datalog-query datalog-query})))
        where-clauses (:where query-map-form)
        ;;bind-symbols are symbols starting with ? in the datalog-query
        ;;for example, in [:find ?id ?description :where [?id :descriotion ?description]]
        ;;the bind-symbols are ?id and ?description
        ;;the map bind-symbol->attr maps the symbol to the attribute which in this example is
        ;;{?id :id ?description :description}
        [entity-ids bind-symbol->attr] (loop [clauses where-clauses
                                              e-ids #{:init} ;; :init is removed when the loop completes
                                              bind-symbol->attr {}]
                                         (let [[e a v t op :as clause] (first clauses)
                                               bind-symbol->attr (if (symbol? e)
                                                                   (assoc bind-symbol->attr e :id)
                                                                   bind-symbol->attr)]

                                           (cond
                                             (empty? clause) [e-ids bind-symbol->attr]
                                             (and (symbol? e)
                                                  (keyword? a)
                                                  (symbol? v)) (let [avet (:avet db)
                                                                     datoms (avet a)
                                                                     matching-ids (set (map first datoms))
                                                                     bind-symbol->attr (assoc bind-symbol->attr v a)]
                                                                 (recur-next-clauses))
                                             (and (symbol? e)
                                                  (keyword? a)
                                                  (regex? v)) (let [e-v-pairs (e-v-pairs db a) 
                                                                    regex v
                                                                    matching-ids (set (keep (fn [[id val]]
                                                                                              (when (re-find regex val)
                                                                                                id))
                                                                                            e-v-pairs))]
                                                                (recur-next-clauses))
                                             (and (symbol? e)
                                                  (keyword? a)
                                                  (lambda? v)) (let [e-v-pairs (e-v-pairs db a)
                                                                     predicate (eval v)
                                                                     matching-ids (set (keep (fn [[id val]]
                                                                                               (when (predicate val)
                                                                                                 id))
                                                                                             e-v-pairs))]
                                                                 (recur-next-clauses))
                                             (and (symbol? e)
                                                  (keyword? a)) (let [aevt (:aevt db)
                                                                      datoms (aevt v)
                                                                      matching-ids (set (map first datoms))]
                                                                  (recur-next-clauses))
                                             :else [e-ids bind-symbol->attr])))]
    (mapv (fn [id]
            (let [this-entity (entity db id)
                  bind-symbols (:find query-map-form)]
              (mapv (fn [a-symbol]
                      (let [attr (bind-symbol->attr a-symbol)]
                        (this-entity attr)))
                    bind-symbols)))
          entity-ids)))

(comment
  (mount/start)
  (mount/stop)
  
  (transact tx-queue [[42 :first-name "sonny"]
                      [42 :last-name "To"]
                      [42 :age 100]
                      [42 :email "son.c.to@gmail.com"]])

  (transact tx-queue [[42 :email "son.c.to@gmail.com5"]])

  (transact tx-queue [[42 :color :red]])

  (transact tx-queue [[42 :url "http://www.foo4.com"]])
  (transact tx-queue [[42 :image "http://www.pix.com"]])
  (transact tx-queue [[42 :sku "abc-sku"]])

  (entity->datoms {:id 1
                   :person/name "sonny"
                   :person/age 1
                   :person/email "sonny@foobar.com"})
  (transact tx-queue [{:id 10
                       :type "couch"
                       :url "https://www.gannett-cdn.com/presto/2021/02/18/USAT/428c9034-7bba-4e97-a055-db2c32082003-cloud-couch-hero.jpg?width=660&height=372&fit=crop&format=pjpg&auto=webp"
                       :image-url "https://www.gannett-cdn.com/presto/2021/02/18/USAT/428c9034-7bba-4e97-a055-db2c32082003-cloud-couch-hero.jpg?width=660&height=372&fit=crop&format=pjpg&auto=webp"
                       :material "leather"
                       :price 10000.0
                       :description "11 affordable alternatives to the $10,000 couch that's blowing up on TikTok"
                       :hex-color "#FFFFF"}
                      [1 :age 1]
                      ])

  (transact tx-queue [[1 :age 10]])
  
  (q '[:find ?id ?email :where
       [?id :first-name "sonny"]
       [?id :email ?email]] @db)

  (q '[:find ?id ?age :where [?id :age ?age]
       ] @db)
  
  (require '[taoensso.timbre.appenders.core :as appenders])
  (log/merge-config! {:min-level :info
                      :middleware [(fn [data]
                                     (update data :vargs (partial mapv #(if (string? %)
                                                                          %
                                                                          (with-out-str (pp/pprint %))))))]
                      :appenders {:println {:enabled? false}
                                  :catalog (merge (appenders/spit-appender {:fname (let [log-dir (or (System/getenv "LOG_DIR") ".")]
                                                                                     (str  log-dir "/debug.log"))})
                                                  {:min-level :info
                                                   :level :info})}})

  (log/set-level! :info)

  (q '[:find ?id ?material
       :where [?id :type "couch"]
       [?id :image-url ?image-url]
       ;;[?id :material "leather"]
       [?id :material ?material]]
     @db)

  (q '[:find  ?price ?id ?hex-color
       :where [?id :type "couch"]
       [?id :material ?material]
       [?id :hex-color ?hex-color]
       ;;[?id :price 1499.0]
       [?id :price ?price]]
     @db)

  (q '[:find ?id ?description
       :where [?id :type "couch"]
       [?id :description #"supports"]
       [?id :description (fn [v]
                           (pos? (.indexOf v "Sven")))]
       [?id :description ?description]]
     @db)
  

  (q (list-form->map-form '[:find ?id :where [?id :description #"sofa"]])
     @db)
  
  (q {:find ['?id], :where ['[?id :description #"sofa"]]}
     @db)

  (let [color "#141D1F"]
    (q {:find ['?id],
        :where [['?id :description #"sofa"]
                ['?id :hex-color(fn [hex-color]
                                  (let [r (= hex-color color)]
                                    
                                    r)
                                  
                                  )]]}
       @db
       ))


  )

