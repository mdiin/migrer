(ns migrer.core
  "Migrer

  Database migrations that do not defeat version control.

  == Usage examples ==

  With three files on the classpath under `migrations/`;

  - File `V001__create_users_table.sql` containing:

  CREATE TABLE public.users (id serial NOT NULL, name text NOT NULL, email text);

  - File `S001__seed_users_table.sql` containing:

  INSERT INTO public.users (name, email)
  VALUES
    ('John Doe', NULL),
    ('Jane Dizzle', 'jd@example.com');

  - File `R001__users_with_email.sql` containing:

  CREATE OR REPLACE VIEW public.users_with_email AS (
    SELECT * FROM public.users WHERE email IS NOT NULL
  );

  The following code will run the migrations in sequence order, applying first the V
  migrations with any S migrations with the same sequence number interleaved, and
  finally the R migrations in sequence order.

  (require 'migrer.core)
  (def jdbc-connection-map {...}) ;; See clojure.java.jdbc docs
  (migrer.core/migrate! jdbc-connection-map) ;; => [\"migrations/V001__create_users_table.sql\"
                                                   \"migrations/S001__seed_users_table.sql\"
                                                   \"migrations/R001__users_with_email.sql\"]

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
   [datascript.core :as d])
  (:import
   [java.lang Exception]
   [java.security MessageDigest]))

(def schema
  {:migration.meta/id {:db/unique :db.unique/identity}
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

(defn- initialise-facts
  [s]
  (let [conn (d/create-conn s)]
    (d/transact! conn [{:db/ident :migration.type/versioned}
                       {:db/ident :migration.type/repeatable}
                       {:db/ident :migration.type/seed}
                       {:db/ident :migration.type/invalid}])
    conn))

(def facts (initialise-facts schema))

(comment
  (let [conn (initialise-facts schema)]
    (d/transact! conn [{:migration.meta/id "foobar"
                        :migration.meta/run? false
                        :migration.meta/type :migration.type/versioned
                        :migration.meta/version "001"
                        :migration.raw/filename "V001__create_schema.sql"
                        :migration.raw/sql "create schema foobar"}
                       {:migration.meta/id "bazbar"
                        :migration.meta/type :migration.type/repeatable
                        :migration.meta/run? true
                        :migration.meta/dependencies [[:migration.meta/id "foobar"]]
                        :migration.raw/filename "R__bazbar_view.sql"
                        :migration.raw/sql "create view bazbar as (...)"}])
    (d/q '[:find ?migration ?filename
           :where
           #_[?migrationA :migration.meta/id ?id]
           #_[?migrationB :migration.meta/dependencies ?migrationA]
           [?migration :migration.meta/type :migration.type/repeatable]
           [?migration :migration.meta/run? true]
           [?migration :migration.raw/filename ?filename]
           ]
         @conn)))

(defn- compare-migration-maps
  [m1 m2]
  (let [{m1-version :migrations/version} m1
        {m2-version :migrations/version} m2]
    (case [(:migrations/type m1) (:migrations/type m2)]
      [:versioned :versioned] (compare m1-version m2-version)
      [:versioned :seed] (let [comparison (compare m1-version m2-version)]
                           (if (= comparison 0) -1 comparison))
      [:versioned :repeatable] (compare m1-version m2-version)
      [:seed :seed] (compare m1-version m2-version)
      [:seed :versioned] (let [comparison (compare m1-version m2-version)]
                           (if (= comparison 0) 1 comparison))
      [:seed :repeatable] (let [comparison (compare m1-version m2-version)]
                            (if (= comparison 0) 1 comparison))
      [:repeatable :repeatable] (compare m1-version m2-version)
      [:repeatable :versioned] (compare m1-version m2-version)
      [:repeatable :seed] (let [comparison (compare m1-version m2-version)]
                            (if (= comparison 0) -1 comparison)))))

(def migration-map-comparator (proxy [java.util.Comparator] []
                                (compare [o1 o2]
                                  (compare-migration-maps o1 o2))))

(defn- read-file
  [path]
  (slurp path))

(defn- read-resource
  [path]
  (slurp (io/resource path)))

(defn- extract-from-path
  [path]
  (-> path
      (str/split #"/")
      (last)
      (->> (re-matches (re-pattern (str "^(\\w)(\\d+)__(.+).sql$"))))))

(defn- migration-map
  "Given a path, create a map with version and migration type extracted, and
  data slurped."
  [read-sql-fn root path]
  (let [[filename type version description] (extract-from-path path)]
    (assert (some #{"V" "R" "S"} [type]) (str "Unknown migration type: " type ". Supported types are [\"V\"ersioned \"S\"eed \"R\"epeatable]"))
    (let [migration-type (case type
                           "V" :versioned
                           "S" :seed
                           "R" :repeatable)]
      #:migrations{:type migration-type
                   :filename filename
                   :sql (read-sql-fn (str root "/" filename))
                   :description description
                   :version version})))

(defn- md5sum
  "Attribution: https://gist.github.com/jizhang/4325757"
  [^String s]
  (->> s
       (.getBytes)
       (.digest (MessageDigest/getInstance "MD5"))
       (BigInteger. 1)
       (format "%032x")))

(defn- extract-meta
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

(comment
  (extract-meta "create table foobar (t boolean);")
  (extract-meta "{:id \"abc\"}\ncreate table foobar (t boolean);")
  (extract-meta "-- whatever vomment\n-- more comment\ncreate table foobar (t boolean);")
  (extract-meta "-- whatever vomment\n-- more comment\ncreate table foobar\n(\nt boolean\n);")
  )

(defn- gather-facts
  [root migration-filenames exclusions repeatable-hashes read-sql-fn]
  (doseq [migration-file-path migration-filenames]
      (let [[filename type version description] (extract-from-path migration-file-path)
            migration-contents (read-sql-fn (str root "/" filename))
            {:keys [dependencies id pre post sql]} (extract-meta migration-contents)]
        (d/transact! facts [{:migration.meta/id id
                             :migration.meta/pre pre
                             :migration.meta/post post
                             :migration.meta/version version
                             :migration.meta/description description
                             :migration.meta/run? (if (= type "R")
                                                    (= (md5sum sql) (get repeatable-hashes filename))
                                                    (not (some exclusions filename)))
                             :migration.meta/type (condp = type
                                                    "V" :migration.type/versioned
                                                    "R" :migration.type/repeatable
                                                    "S" :migration.type/seed
                                                    :migration.type/invalid)
                             :migration.raw/sql sql
                             :migration.raw/dependencies dependencies
                             :migration.raw/filename filename}]))))

(defn- populate-dependencies
  []
  (let [raw-deps (d/q '[:find ?e ?deps
                        :where
                        [?e :migration.raw/dependencies ?deps]]
                      @facts)
        tx-data (map (fn [e deps]
                       (let [deps-as-refs (map (fn [dep] [:migration.meta/id dep]))]
                         {:db/ident e :migration.meta/dependencies deps-as-refs})))]
    (d/transact! facts tx-data)))

(defn- read-migrations-fs
  "Populates fact database with metadata about all migrations in migration root, read from filesystem."
  [root exclusions repeatable-hashes]
  (let [dir-file (io/file root)]
    (gather-facts root
                  (into []
                        (comp
                         (filter #(not (.isDirectory %)))
                         (map #(.getName %)))
                        (.listFiles dir-file))
                  repeatable-hashes
                  read-file)
    (populate-dependencies)))

(defn- read-migrations-resources
  "Populates fact database with metadata about all migrations in migration root, read from classpath."
  {:impl-doc "Uses getResourceAsStream on the context classloader because the regular
             clojure.java.io/resource call does not handle directories. We use the
             resource stream to read the names of all resources inside a resource dir."}
  [root exclusions repeatable-hashes]
  (with-open [rdr (io/reader
                   (.getResourceAsStream
                    (.. Thread currentThread getContextClassLoader)
                    root))]
    (gather-facts root
                  (line-seq rdr)
                  repeatable-hashes
                  read-resource)
    (populate-dependencies)))

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
        (if (= type :repeatable)
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
                                                  [:version "varchar(32)" "NOT NULL"]
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
         _ (println opts)
         migrations (try
                      (if (:migrer/use-classpath? opts)
                        (read-migrations-resources (:migrer/root opts) (exclusions conn table-name) (repeatable-hashes conn table-name))
                        (read-migrations-fs (:migrer/root opts) (exclusions conn table-name) (repeatable-hashes conn table-name)))
                      (catch Exception e
                        (if (str/includes? (ex-message e) table-name)
                          (do
                            (println (str "=== Database migrations table not initialized. ==="))
                            (println "")
                            (println (str " * Are you certain that \"" table-name "\" is the correct table?"))
                            (println "")
                            (println "===")
                            ::exception)
                          (throw e))))]
     (cond
       (= ::exception migrations) (println "")
       (empty? migrations) (println "=== Database is already up to date. ===")
       :else (do
               (println (str "=== Performing " (count migrations) " migrations: ==="))
               (println "")
               (reduce (fn [acc migration-map]
                         (if (= (perform-migration-sql
                                 conn
                                 table-name
                                 migration-map
                                 (:migrer/log-fn opts))
                                :result/error)
                           (reduced acc)
                           (conj acc migration-map)))
                       []
                       migrations)
               (println "")
               (println "=== Finished! ==="))))))
