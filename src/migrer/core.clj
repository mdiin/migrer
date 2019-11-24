(ns migrer.core
  "Migrer

  Database migrations that do not defeat version control.

  == Usage examples ==

  With three files on the classpath under `migrations/`;

  - File `V001__create_users_table.sql` containing:

  CREATE TABLE public.users (id serial NOT NULL, name text NOT NULL, email text);

  - File `V002__seed_users_table.sql` containing:

  INSERT INTO public.users (name, email)
  VALUES
    ('John Doe', NULL),
    ('Jane Dizzle', 'jd@example.com');

  - File `R001__users_with_email.sql` containing:

  {:dependencies #{\"V001__create_users_table.sql\"}}
  CREATE OR REPLACE VIEW public.users_with_email AS (
    SELECT * FROM public.users WHERE email IS NOT NULL
  );

  The following code will run the migrations in dependency order, i.e. lower
  sequence numbers first, and their dependents second. In this example, that
  means the order is:

  1. V001__create_users_table.sql
  2. R__users_with_eamil.sql
  3. V002__seed_users_table.sql

  (require 'migrer.core)
  (def jdbc-connection-map {...}) ;; See clojure.java.jdbc docs
  (migrer.core/migrate! jdbc-connection-map) ;; => [\"migrations/V001__create_users_table.sql\"
                                                   \"migrations/V001__seed_users_table.sql\"
                                                   \"migrations/R__users_with_email.sql\"]

  After this the database will contain the entities specified in the migrations,
  and a special table `migrer.migrations` containing a record of each applied migration
  file.

  The invocation of `migrer.core/migrate!` can optionally be supplied with a path to the
  migrations and a map of options. Currently no options are available.
  "
  (:require
   [clojure.java.io :as io]
   [clojure.java.jdbc :as jdbc]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [clojure.test :as tst :refer [with-test]]
   [datascript.core :as d])
  (:import
   [java.lang Exception]
   [java.security MessageDigest]))

(def schema
  {:migration.meta/id {:db/unique :db.unique/identity}
   :migration.meta/description {:db/cardinality :db.cardinality/one}
   :migration.meta/version {:db/cardinality :db.cardinality/one}
   :migration.meta/dependencies {:db/cardinality :db.cardinality/many
                                 :db/valueType :db.type/ref}
   :migration.meta/pre {:db/cardinality :db.cardinality/many}
   :migration.meta/post {:db/cardinality :db.cardinality/many}
   :migration.meta/run? {:db/cardinality :db.cardinality/one}
   :migration.meta/type {:db/cardinality :db.cardinality/one
                         :db/valueType :db.type/ref}
   :migration.raw/filename {:db/cardinality :db.cardinality/one}
   :migration.raw/dependencies {:db/cardinality :db.cardinality/many}
   :migration.raw/sql {:db/cardinality :db.cardinality/one}})

(defn- lt?
  [a b]
  (= -1 (compare a b)))

(def fact-rules
  '[[(earlier-version ?e1 ?e2)
     [?e1 :migration.meta/version ?e1-v]
     [?e2 :migration.meta/version ?e2-v]
     [(migrer.core/lt? ?e1-v ?e2-v)]]
    [(dependency-of ?e1 ?e2)
     [?e1 :migration.meta/dependencies ?e2]]
    [(dependency-of ?e1 ?e3)
     [?e2 :migration.meta/dependencies ?e3]
     (dependency-of ?e1 ?e2)]
    [(prior-to ?e1 ?e2)
     (earlier-version ?e1 ?e2)]
    [(prior-to ?e1 ?e2)
     (dependency-of ?e2 ?e1)]
    [(root-level? ?e ?ep)
     [?e :migration.meta/id]
     (not
      (prior-to ?ep ?e)
      (should-run? ?ep))]
    [(should-run? ?e)
     [?e :migration.meta/run? true]]
    [(should-run? ?e)
     [?e :migration.meta/type :migration.type/repeatable]
     [?e :migration.meta/run? false]
     (prior-to ?e-prior ?e)
     (should-run? ?e-prior)]])

(defn- initialise-facts
  [s]
  (let [conn (d/create-conn s)]
    (d/transact! conn [{:db/ident :migration.type/versioned}
                       {:db/ident :migration.type/repeatable}
                       {:db/ident :migration.type/seed}
                       {:db/ident :migration.type/invalid}])
    conn))

(defn- read-file
  [path]
  (slurp path))

(defn- read-resource
  [path]
  (slurp (io/resource path)))

(with-test
  (defn extract-from-path
    [path]
    (-> path
        (str/split #"/")
        (last)
        (->> (re-matches (re-pattern (str "^(\\w)(\\d+)?__(.+).sql$"))))))

  (tst/is (= ["V999__foobar.sql" "V" "999" "foobar"]
             (extract-from-path "V999__foobar.sql")))
  (tst/is (= ["R__do_repeatable_magic.sql" "R" nil "do_repeatable_magic"]
             (extract-from-path "R__do_repeatable_magic.sql")))
  (tst/is (= nil
             (extract-from-path "onetwothree.sql")))
  (tst/is (= nil
             (extract-from-path "Vonehunderd__sqls.sql")))
  (tst/is (= nil
             (extract-from-path "Rotten__birds__view.sql"))))

(defn- md5sum
  "Attribution: https://gist.github.com/jizhang/4325757"
  [^String s]
  (->> s
       (.getBytes)
       (.digest (MessageDigest/getInstance "MD5"))
       (BigInteger. 1)
       (format "%032x")))

(with-test
  (defn extract-meta
    "Extract metadata from the lines of an SQL file.

  Any metadata must be specified as a single EDN map at the top of the file."
    [sql]
    (with-open [string-reader (java.io.StringReader. sql)
                edn-reader (java.io.PushbackReader. string-reader)]
      (let [metadata (clojure.edn/read edn-reader)
            sql (clojure.string/join "\n" (line-seq (clojure.java.io/reader string-reader)))]
        (if (map? metadata)
          (merge metadata {:sql sql})
          {:sql (str metadata " " sql)}))))

  (let [input "create table foobar (t boolean);"]
    (tst/is (= {:sql input}
               (extract-meta input))))
  (let [sql "\ncreate table foobar (t boolean);"
        input (str "{:id \"abc\"}" sql)]
    (tst/is (= {:id "abc" :sql sql}
               (extract-meta input))))
  (let [input "-- whatever vomment\n-- more comment\ncreate table foobar (t boolean);"]
    (tst/is (= {:sql input}
               (extract-meta input))))
  (let [input "-- whatever vomment\n-- more comment\ncreate table foobar\n(\nt boolean\n);"]
    (tst/is (= {:sql input}
               (extract-meta input)))))

(defn- gather-facts
  [fact-db root migration-filenames exclusions repeatable-hashes read-sql-fn]
  (doseq [migration-file-path migration-filenames]
    (let [[filename type version path-description :as gg] (extract-from-path migration-file-path)
          migration-contents (read-sql-fn (str root "/" filename))
          {:keys [dependencies id pre post sql meta-description]} (extract-meta migration-contents)
          doc {:migration.meta/id (or id filename)
               :migration.meta/pre (into #{} pre)
               :migration.meta/post (into #{} post)
               :migration.meta/description (or meta-description path-description)
               :migration.meta/run? (if (= type "R")
                                      (not= (md5sum sql) (get repeatable-hashes filename))
                                      (not (some exclusions filename)))
               :migration.meta/type (condp = type
                                      "V" :migration.type/versioned
                                      "R" :migration.type/repeatable
                                      "S" :migration.type/seed
                                      :migration.type/invalid)
               :migration.raw/dependencies (into #{} dependencies)
               :migration.raw/sql sql
               :migration.raw/filename filename}]
      (d/transact! fact-db [(if version
                              (merge doc {:migration.meta/version version})
                              doc)]))))

(with-test
  (defn populate-dependencies
    [fact-db]
    (let [raw-deps (d/q '[:find ?e (aggregate ?agg ?deps)
                          :in $ ?agg
                          :where
                          [?e :migration.raw/dependencies ?deps]]
                        @fact-db
                        identity)
          tx-data (mapcat (fn [[e deps]]
                            (map (fn [dep]
                                   [:db/add e
                                    :migration.meta/dependencies [:migration.meta/id dep]])
                                 deps))
                       raw-deps)]
      (d/transact! fact-db tx-data)))

  (let [conn (initialise-facts schema)]
    (d/transact! conn [{:migration.meta/id "R__a.sql"
                        :migration.meta/description "foobar"
                        :migration.meta/run? true
                        :migration.meta/type :migration.type/repeatable
                        :migration.raw/dependencies #{"V001__a.sql"}
                        :migration.raw/sql "some sql"
                        :migration.raw/filename "R__a.sql"}
                       {:migration.meta/id "V001__a.sql"
                        :migration.meta/version "001"
                        :migration.meta/description "bazbar"
                        :migration.meta/run? true
                        :migration.meta/type :migration.type/versioned
                        :migration.raw/sql "some sql"
                        :migration.raw/filename "V001__a.sql"}])
    (populate-dependencies conn)
    (tst/is (= "V001__a.sql"
               (d/q '[:find ?did .
                      :in $ ?id
                      :where
                      [?e :migration.meta/id ?id]
                      [?e :migration.meta/dependencies ?d]
                      [?d :migration.meta/id ?did]]
                    @conn
                    "R__a.sql")))))

(defn- read-migrations-fs
  "Populates fact database with metadata about all migrations in migration root, read from filesystem."
  [fact-db root exclusions repeatable-hashes]
  (let [dir-file (io/file root)]
    (gather-facts fact-db
                  root
                  (into []
                        (comp
                         (filter #(not (.isDirectory %)))
                         (map #(.getName %)))
                        (.listFiles dir-file))
                  exclusions
                  repeatable-hashes
                  read-file)
    (populate-dependencies fact-db)))

(defn- read-migrations-resources
  "Populates fact database with metadata about all migrations in migration root, read from classpath."
  {:impl-doc "Uses getResourceAsStream on the context classloader because the regular
             clojure.java.io/resource call does not handle directories. We use the
             resource stream to read the names of all resources inside a resource dir."}
  [fact-db root exclusions repeatable-hashes]
  (with-open [rdr (io/reader
                   (.getResourceAsStream
                    (.. Thread currentThread getContextClassLoader)
                    root))]
    (gather-facts fact-db
                  root
                  (line-seq rdr)
                  exclusions
                  repeatable-hashes
                  read-resource)
    (populate-dependencies fact-db)))

(defn- exclusions
  [conn table-name]
  (let [performed-migrations (jdbc/query conn [(str "SELECT filename FROM " table-name " WHERE type <> 'repeatable';")])]
    (into #{} (map :filename) performed-migrations)))

(defn- repeatable-hashes
  [conn table-name]
  (let [repeatable-hashes (jdbc/query conn [(str "SELECT filename, hash, performed_at AS \"performed-at\" FROM " table-name " WHERE type = 'repeatable';")])]
    (into {}
          (map (fn [[filename [entry]]]
                 [filename (:hash entry)]))
          (group-by :filename repeatable-hashes))))

(defn- perform-migration-sql
  [conn migrations-table {sql :migrations/sql :as migration-map} log-fn]
  (log-fn {:event/type :start} migration-map)
  (try
    (log-fn {:event/type :progress :event/data sql} migration-map)
    (let [time-start (.. (new java.util.Date) (getTime))]
      (jdbc/execute! conn [sql])
      (log-fn {:event/type :done
               :event/data {:ms (- (.. (new java.util.Date) (getTime)) time-start)}}
              migration-map)
      (let [{type :migrations/type
             version :migrations/version
             filename :migrations/filename} migration-map]
        (if (= type :migration.type/repeatable)
          (jdbc/with-db-transaction [tx-conn conn]
            (jdbc/execute! tx-conn [(str "UPDATE " migrations-table " SET status = 'invalidated' WHERE type = 'repeatable' AND filename = ?")
                                    filename])
            (jdbc/execute! tx-conn [(str "INSERT INTO " migrations-table " (type, version, filename, hash, status, performed_at) VALUES (?, ?, ?, ?, 'performed', ?)")
                                 (name type) version filename (md5sum sql) (java.sql.Date. (.getTime (java.util.Date.)))]))
          (jdbc/execute! conn [(str "INSERT INTO " migrations-table " (type, version, filename, status, performed_at) VALUES (?, ?, ?, 'performed', ?)")
                               (name type) version filename (java.sql.Date. (.getTime (java.util.Date.)))])))
      :result/done)
    (catch Exception e
      (log-fn {:event/type :error :event/data (.getMessage e)}
              migration-map)
      :result/error)))

(defn- log-migration
  [event migration-map]
  (let [{event-type :event/type} event
        {type :migrations/type
         version :migrations/version
         sql :migrations/sql
         description :migrations/description} migration-map]
    (case event-type
      (:start) (println (str "=== " event-type " === [" type " | " description " @ " version "]"))
      (:progress) (println (:event/data event))
      (:error) (println (str "=== ERROR REPORT ===\n\n"
                             (:event/data event)
                             "\n\n"
                             "=== ERROR REPORT END ==="
                             "\n"))
      (:done) (println (str "=== " event-type " ===\n"
                            "=== time: " (get-in event [:event/data :ms]) "ms ==="
                            "\n")))))

(def default-options {:migrer/root "migrations/"
                      :migrer/table-name :migrations
                      :migrer/use-classpath? true
                      :migrer/log-fn #'log-migration
                      :init/conditional-create-table? true
                      :init/conditional-create-index? true})

(defn init!
  "Initialise migrations metadata on database.

  options are the same as for `migrate!`"
  ([conn]
   (init! conn default-options))
  ([conn options]
   (let [opts (merge default-options options)]
     (jdbc/execute! conn [(jdbc/create-table-ddl (:migrer/table-name opts)
                                                 [[:type "varchar(32)" "NOT NULL"]
                                                  [:version "varchar(32)"]
                                                  [:filename "varchar(512)" "NOT NULL"]
                                                  [:hash "varchar(256)"]
                                                  [:status "varchar(32) NOT NULL"]
                                                  [:performed_at "timestamp" "NOT NULL"]]
                                                 {:conditional? (:init/conditional-create-table? opts)})])
     (let [table-name-str (name (:migrer/table-name opts))
           conditional? (:init/conditional-create-index? opts)
           index-name (str table-name-str "_type_filename_idx")]
       (jdbc/execute! conn [(if (fn? conditional?)
                              (let [sql (str "CREATE INDEX "
                                             index-name
                                             " ON "
                                             table-name-str
                                             " (type, filename);")]
                                (conditional? index-name sql))
                              (str "CREATE INDEX "
                                   (when conditional?
                                     "IF NOT EXISTS ")
                                   index-name
                                   " ON "
                                   table-name-str
                                   " (type, filename);"))])))))

(with-test
 (defn dependency-order
   [migrations]
   (sort-by identity
            (fn cmp [[a-id a-deps] [b-id b-deps]]
              (cond
                (b-deps a-id) -1
                (a-deps b-id) 1
                :else (compare a-id b-id)))
            migrations))

  (tst/is (= (list [1 #{}] [2 #{1}])
             (dependency-order (list [2 #{1}] [1 #{}]))))
  (tst/is (= (list [6 #{}] [7 #{6}] [8 #{7}] [9 #{8}] [10 #{9}] [11 #{10}])
             (dependency-order (list [7 #{6}] [10 #{9}] [6 #{}] [8 #{7}] [11 #{10}] [9 #{8}])))))

(with-test
  (defn migration-eids-in-application-order
    [all-facts]
    (dependency-order
     (d/q '[:find ?e (aggregate ?agg ?e-dep)
            :in $ % ?agg
            :where
            (or
             (and
              (should-run? ?e)
              (root-level? ?e ?e-dep))
             (and
              (should-run? ?e)
              (prior-to ?e-dep ?e)
              (should-run? ?e-dep)))]
          all-facts
          fact-rules
          #(into #{} (filter (comp not nil?)) %))))

  (let [conn (initialise-facts schema)]
    (d/transact! conn [{:migration.meta/id "foobar"
                        :migration.meta/run? false
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/version "001"
                        :migration.raw/filename "V001__create_schema.sql"
                        :migration.raw/sql "create schema foobar"}
                       {:migration.meta/id "snaz"
                        :migration.meta/run? true
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/version "002"
                        :migration.raw/filename "V002__create_table.sql"
                        :migration.raw/sql "create table sometable (...);"}
                       {:migration.meta/id "bazbar"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "foobar"]]
                        :migration.raw/filename "R__bazbar_view.sql"
                        :migration.raw/sql "create view bazbar as (...)"}])

    (tst/is (= ["snaz" "bazbar"]
               (map
                (comp :migration.meta/id
                      (partial d/entity @conn)
                      first)
                (migration-eids-in-application-order @conn))))))

(defn migrate!
  "Runs any pending migrations, returning a vector of the performed migrations in order.

  options is a map with any of these keys:

  - `:migrer/table-name`: The name of the table where migrer will store metadata about migrations [\"migrations\"]
  - `:migrer/root`: Where on the classpath migrer should look for migrations [\"migrations/\"]
  - `:migrer/log-fn`: A function of two arguments (event, migration-map) to use for logging migration reports [`migrer.core/log-migration`]"
  ([conn]
   (migrate! conn default-options))
  ([conn options]
   (let [opts (merge default-options options)
         table-name (name (:migrer/table-name opts))
         fact-db (initialise-facts schema)
         _ (println opts)
         gather-facts-result (try
                               (if (:migrer/use-classpath? opts)
                                 (read-migrations-resources fact-db (:migrer/root opts) (exclusions conn table-name) (repeatable-hashes conn table-name))
                                 (read-migrations-fs fact-db (:migrer/root opts) (exclusions conn table-name) (repeatable-hashes conn table-name)))
                               (catch Exception e
                                 (let [ex-msg (ex-message e)]
                                   (if (and ex-msg (str/includes? ex-msg table-name))
                                     (do
                                       (println (str "=== Database migrations table not initialized. ==="))
                                       (println "")
                                       (println (str " * Are you certain that \"" table-name "\" is the correct table?"))
                                       (println "")
                                       (println "===")
                                       ::exception)
                                     (throw e)))))
         all-facts (deref fact-db)
         migration-eids (migration-eids-in-application-order all-facts)
         _ (println migration-eids)]
     (cond
       (= ::exception gather-facts-result) (println "")
       (empty? migration-eids) (println "=== Database is already up to date. ===")
       :else (do
               (println (str "=== Performing " (count migration-eids) " migrations: ==="))
               (println "")
               (reduce (fn [acc [migration-eid _]]
                         (let [entity (d/entity all-facts migration-eid)
                               migration-type (:db/ident (d/entity all-facts (get-in entity [:migration.meta/type :db/id])))
                               migration-map {:migrations/type migration-type
                                              :migrations/filename (:migration.raw/filename entity)
                                              :migrations/sql (:migration.raw/sql entity)
                                              :migrations/description (:migration.meta/description entity)
                                              :migrations/version (:migration.meta/version entity)}]
                           (println migration-map)
                           (if (= (perform-migration-sql
                                   conn
                                   table-name
                                   migration-map
                                   (:migrer/log-fn opts))
                                  :result/error)
                             (reduced acc)
                             (conj acc migration-map))))
                       []
                       migration-eids)
               (println "")
               (println "=== Finished! ==="))))))
