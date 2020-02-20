(ns migrer.core
  "Migrer

  Database migrations that do not defeat version control.

  == Usage examples ==

  With three files on the classpath under `migrations/`;

  - File `V__create_users_table.sql` containing:

  ```
  CREATE TABLE public.users (id serial NOT NULL, name text NOT NULL, email text);
  ```

  - File `R__users_named_john.sql` containing:

  ```
  {:dependencies #{\"V__create_users_table.sql\"}}
  CREATE OR REPLACE VIEW public.users_named_john AS (
    SELECT * FROM public.users WHERE name ILIKE 'John %'
  );
  ```

  - File `R__users_with_email.sql` containing:

  ```
  {:dependencies #{\"V__create_users_table.sql\"}}
  CREATE OR REPLACE VIEW public.users_with_email AS (
    SELECT * FROM public.users WHERE email IS NOT NULL
  );
  ```

  The following code will run the migrations in dependency order, i.e. lower
  sequence numbers first, and their dependents second. In this example, that
  means the order is:

  1. V001__create_users_table.sql
  2. R__users_with_email.sql
  3. R__users_named_john.sql

  Note: The order of 2. and 3. is undefined because they have the same number of
  dependencies.

  (require 'migrer.core)
  (def jdbc-connection-map {...}) ;; See clojure.java.jdbc docs
  (migrer.core/migrate! jdbc-connection-map) ;; => [\"migrations/V__create_users_table.sql\"
                                                   \"migrations/R__users_named_john.sql\"
                                                   \"migrations/R__users_with_email.sql\"]

  After this the database will contain the entities specified in the migrations,
  and a special table `migrer.migrations` containing a record of each applied migration
  file.

  The invocation of `migrer.core/migrate!` can optionally be supplied with a path to the
  migrations and a map of options. See `default-options` for the available keys and their
  defaults.
  "
  (:require
   [migrer.facts :as facts]
   [clojure.java.io :as io]
   [clojure.java.jdbc :as jdbc]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [clojure.set :as set]
   [clojure.test :as tst :refer [with-test]]
   [datascript.core :as d])

  (:import
   [java.lang Exception]
   [java.security MessageDigest]))

(defn- read-file
  [path]
  (slurp path))

(defn- read-resource
  [path]
  (slurp (io/resource path)))

(defn- version->int
  [[_ _ v _ :as orig]]
  (if v
    (try
      (assoc orig (Integer/parseInt v) 2)
      (catch Exception e
        orig))
    orig))

(with-test
  (defn extract-from-path
    [path]
    (-> path
        (str/split #"/")
        (last)
        (->> (re-matches (re-pattern (str "^(\\w)(\\d+)?__(.+).sql$")))
             (version->int))))

  (tst/is (= ["V__foobar.sql" "V" nil "foobar"]
             (extract-from-path "V__foobar.sql")))
  (tst/is (= ["V999__foobar.sql" "V" "999" "foobar"]
             (extract-from-path "V999__foobar.sql")))
  (tst/is (= ["R__do_repeatable_magic.sql" "R" nil "do_repeatable_magic"]
             (extract-from-path "R__do_repeatable_magic.sql")))
  (tst/is (= ["S__seed_some_stuff.sql" "S" nil "seed_some_stuff"]
             (extract-from-path "S__seed_some_stuff.sql")))
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
  (defn comment-block
    [s]
    (rest
     (re-matches #"^(?:/\*(?s:(.+))\*/)?\s?(?s:(.*))$" s)))

  (let [expected "\nfoobar\n"
        input (str "/*" expected "*/\ncreate foobar;")]
    (tst/is (= [expected "create foobar;"] (comment-block input))))
  (let  [expected  "foobar"
         input (str "/*" expected "*/")]
    (tst/is (= [expected ""] (comment-block input))))
  (let [expected "foo\nbar"
        input (str "/*" expected "*/")]
    (tst/is (= [expected ""] (comment-block input))))
  (let [input "create foobatr"]
    (tst/is (= [nil input] (comment-block input))))
  (let [expected "\n{:id \"abc\"}\n"
        sql "create table foobar (t boolean);"
        input (str "/*" expected "*/\n" sql)]
    (tst/is (= [expected sql]
               (comment-block input)))))

(with-test
  (defn extract-meta
    "Extract metadata from the lines of an SQL file.

  Any metadata must be specified as a single EDN map at the top of the file."
    [sql]
    (let [[?clj-form just-sql] (comment-block sql)
          ?metadata (clojure.edn/read-string ?clj-form)]
      (if (map? ?metadata)
        (merge ?metadata {:sql just-sql})
        {:sql sql})))

  (let [input "create table foobar (t boolean);"]
    (tst/is (= {:sql input}
               (extract-meta input))))
  (let [sql "create table foobar (t boolean);"
        input (str "/*\n{:id \"abc\"}\n*/\n" sql)]
    (tst/is (= {:id "abc" :sql sql}
               (extract-meta input))))
  (let [input "-- whatever vomment\n-- more comment\ncreate table foobar (t boolean);"]
    (tst/is (= {:sql input}
               (extract-meta input))))
  (let [input "-- whatever vomment\n-- more comment\ncreate table foobar\n(\nt boolean\n);"]
    (tst/is (= {:sql input}
               (extract-meta input)))))

(with-test
  (defn gather-facts
    [fact-db root migration-filenames exclusions repeatable-hashes read-sql-fn]
    (doseq [migration-file-path migration-filenames]
      (let [[filename type version path-description :as gg] (extract-from-path migration-file-path)
            migration-contents (read-sql-fn (str root "/" filename))
            {:keys [dependencies id sql meta-description]} (extract-meta migration-contents)
            doc {:migration.meta/id (or id filename)
                 :migration.meta/description (or meta-description path-description)
                 :migration.meta/run? (if (= type "R")
                                        (not= (md5sum sql) (get repeatable-hashes filename))
                                        (not (some exclusions [filename])))
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

  (let [fact-db (facts/initialise)
        root "migrations"
        migration-filenames ["V__foobar.sql"]
        exclusions #{"V__foobar.sql"}
        repeatable-hashes {}
        read-sql-fn (fn [_] (str "/*"
                                 "{:dependencies #{}}"
                                 "*/"
                                 "create table foobar();"))]
    (gather-facts fact-db root migration-filenames exclusions repeatable-hashes read-sql-fn)
    (tst/is (= false
               (d/q '[:find ?run .
                      :in $
                      :where
                      [?e :migration.meta/id "V__foobar.sql"]
                      [?e :migration.meta/run? ?run]]
                    @fact-db))
            "Mark excluded migrations as not runnable.")))

(with-test
  (defn populate-dependencies
    [fact-db]
    (let [raw-deps (d/q '[:find ?e (aggregate ?agg ?deps)
                          :in $ ?agg
                          :where
                          [?e :migration.raw/dependencies ?deps]
                          [?ep :migration.meta/id ?deps]]
                        @fact-db
                        identity)
          tx-data (mapcat (fn [[e deps]]
                            (map (fn [dep]
                                   [:db/add e
                                    :migration.meta/dependencies [:migration.meta/id dep]])
                                 deps))
                       raw-deps)]
      (d/transact! fact-db tx-data)))

  (let [conn (facts/initialise)]
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
      (:start) (println (str "=== " event-type " === [" type " | " description (when version (str " @ " version)) "]"))
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
  (defn find-with-wrong-dependency-specs
    [facts]
    (d/q '[:find ?id ?filename ?dep-name
           :in $
           :where
           [?e :migration.raw/dependencies ?dep-name]
           [?e :migration.raw/filename ?filename]
           [?e :migration.meta/id ?id]
           (not [_ :migration.meta/id ?dep-name])]
         facts))

  (let [conn (facts/initialise)]
    (d/transact! conn [{:migration.meta/id "A"}
                       {:migration.meta/id "B"}
                       {:migration.meta/id "C"
                        :migration.raw/filename "C.sql"
                        :migration.raw/dependencies #{"a"}}])
    (tst/is (= #{["C" "C.sql" "a"]}
               (find-with-wrong-dependency-specs
                @conn))
            "Wrong dependency"))

  (let [conn (facts/initialise)]
    (d/transact! conn [{:migration.meta/id "A"}
                       {:migration.meta/id "B"}
                       {:migration.meta/id "C"
                        :migration.raw/filename "C.sql"
                        :migration.raw/dependencies #{"A"}}])
    (tst/is (= #{}
               (find-with-wrong-dependency-specs
                @conn))
            "No wrong dependency")))

(defn- find-all-root-migrations
  [facts]
  (let [res (d/q '[:find [?e ...]
                   :in $ %
                   :where
                   (root-migration ?e)]
                 facts
                 facts/new-rules)]
    (set res)))

(defn- find-next-wave-migrations
  [facts]
  (let [res (d/q '[:find [?e ...]
                   :in $ %
                   :where
                   (next-wave ?e)]
                 facts
                 facts/new-rules)]
    (set res)))

(defn- add-to-wave
  [fact-db eids wave-num]
  (d/transact!
   fact-db
   (map (fn [eid]
          {:db/id eid
           :migration.meta/wave wave-num})
        eids)))

(with-test
  (defn migration-eids-in-application-order
    "Returns a vector of migration waves."
    [fact-db]
    (loop [waves []
           wave (find-all-root-migrations @fact-db)]
      (if (and (< (count waves) 10) (seq wave))
        (do
          (add-to-wave fact-db wave (inc (count waves)))
          (recur
            (conj waves wave)
            (find-next-wave-migrations @fact-db)))
        waves)))

  (let [conn (facts/initialise)]
    (d/transact! conn [{:migration.meta/id "bazbar"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies []
                        :migration.raw/filename "R__bazbar_view.sql"
                        :migration.raw/sql "create view bazbar as (...)"}
                       {:migration.meta/id "S__seed_sometable.sql"
                        :migration.meta/type :migration.type/seed
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "bazbar"]]
                        :migration.raw/filename "S__seed_sometable.sql"
                        :migration.raw/sql "insert into sometable (...) values (...);"}])
    (tst/is (= ["bazbar" "S__seed_sometable.sql"]
               (map
                (comp :migration.meta/id
                      (partial d/entity @conn)
                      first)
                (migration-eids-in-application-order conn)))))

  (let [conn (facts/initialise)]
    (d/transact! conn [{:migration.meta/id "X"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies []
                        :migration.raw/filename "R__bazbar_view.sql"
                        :migration.raw/sql "create view bazbar as (...)"}
                       {:migration.meta/id "A"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/dependencies []}
                       {:migration.meta/id "B"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "A"]
                                                      [:migration.meta/id "X"]]}
                       {:migration.meta/id "C"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "B"]]}
                       {:migration.meta/id "D"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? false
                        :migration.meta/dependencies [[:migration.meta/id "A"]
                                                      [:migration.meta/id "X"]]}])
    (tst/is (= [#{"X" "C"} #{"D"}]
               (map
                 (fn [eids]
                   (into #{}
                         (map (comp :migration.meta/id
                                    (partial d/entity @conn)))
                         eids))
                 (migration-eids-in-application-order conn)))
            "Test waves."))

  (let [conn (facts/initialise)]
    (d/transact! conn [{:migration.meta/id "A"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies []}
                       {:migration.meta/id "B"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies []}
                       {:migration.meta/id "C"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "A"]
                                                      [:migration.meta/id "B"]]}
                       {:migration.meta/id "D"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "B"]]}
                       {:migration.meta/id "E"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "A"]]}
                       {:migration.meta/id "F"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies []}
                       {:migration.meta/id "G"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "C"]
                                                      [:migration.meta/id "F"]
                                                      [:migration.meta/id "D"]]}
                       {:migration.meta/id "H"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "G"]
                                                      [:migration.meta/id "E"]
                                                      [:migration.meta/id "A"]
                                                      [:migration.meta/id "B"]]}
                       {:migration.meta/id "I"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "A"]
                                                      [:migration.meta/id "B"]
                                                      [:migration.meta/id "F"]
                                                      [:migration.meta/id "G"]]}
                       {:migration.meta/id "J"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "B"]
                                                      [:migration.meta/id "C"]]}
                       {:migration.meta/id "K"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "B"]]}
                       {:migration.meta/id "L"
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "C"]
                                                      [:migration.meta/id "F"]
                                                      [:migration.meta/id "H"]]}])
    (tst/is (= [#{"A" "B" "F"} #{"C" "D" "E" "K"} #{"G" "J"} #{"H" "I"} #{"L"}]
               (map
                 (fn [eids]
                   (into #{}
                         (map (comp :migration.meta/id
                                    (partial d/entity @conn)))
                         eids))
                 (migration-eids-in-application-order conn)))
            "Test waves #2.")))

(defn- reduce-wave
  [{:keys [conn facts table-name opts]} wave]
  (loop [acc []
         migration-eids wave]
    (if (seq migration-eids)
      (let [current-migration-eid (first migration-eids)
            remaining-migration-eids (rest migration-eids)
            entity (d/entity facts current-migration-eid)
            migration-type (:db/ident (d/entity facts (get-in entity [:migration.meta/type :db/id])))
            migration-map {:migrations/type migration-type
                           :migrations/filename (:migration.raw/filename entity)
                           :migrations/sql (:migration.raw/sql entity)
                           :migrations/description (:migration.meta/description entity)
                           :migrations/version (:migration.meta/version entity)}]
        (if (= :result/error
               (perform-migration-sql conn
                                      table-name
                                      migration-map
                                      (:migrer/log-fn opts)))
          [:result/error acc]
          (recur (conj acc migration-map)
                 remaining-migration-eids)))
      acc)))

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
         fact-db (facts/initialise)
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
         wrong-dep-specs (find-with-wrong-dependency-specs all-facts)
         migration-waves (migration-eids-in-application-order fact-db)]
     (if (seq wrong-dep-specs)
       (do
         (println "=== Problematic dependency specifications ===")
         (println "")
         (doseq [[id filename dep-spec] wrong-dep-specs]
           (println (str " * - filename: " filename))
           (println (str "   - dependency: " dep-spec))
           (println ""))
         (println "=== END: No migrations were performed ==="))
       (cond
         (= ::exception gather-facts-result) (println "")
         (empty? migration-waves) (println "=== Database is already up to date. ===")
         :else (do
                 (println (str "=== Performing " (count migration-waves) " migration waves (" (count (apply set/union migration-waves)) " migrations total): ==="))
                 (println "")
                 (loop [acc []
                        waves migration-waves]
                   (if (seq waves)
                     (let [current-wave (first waves)
                           remaining-waves (rest waves)
                           wave-result (reduce-wave {:conn conn
                                                     :facts all-facts
                                                     :table-name table-name
                                                     :opts opts}
                                                    current-wave)]
                       (if (= :result/error (first wave-result))
                         (conj acc (rest wave-result))
                         (recur (conj acc wave-result)
                           remaining-waves)))
                     acc))
                 (println "")
                 (println "=== Finished! ===")))))))
