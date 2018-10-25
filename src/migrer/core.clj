(ns migrer.core
  "Migrer

  Database migrations that do not defeat version control.

  == Usage examples ==

  With four files on the classpath under `migrations/`;

  - File `V001__create_database.sql` containing:

  DROP DATABASE IF EXISTS migrer;
  CREATE DATABASE migrer;

  - File `V002__create_users_table.sql` containing:

  CREATE TABLE public.users (id serial NOT NULL, name text NOT NULL, email text);

  - File `S002__seed_users_table.sql` containing:

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
  (migrer.core/migrate jdbc-connection-map) ;; => [\"migrations/V001__create_database.sql\"
                                                   \"migrations/V002__create_users_table.sql\"
                                                   \"migrations/S002__seed_users_table.sql\"
                                                   \"migrations/R001__users_with_email.sql\"]

  After this the `migrer` database will contain the entities specified in the migrations,
  and a special table `migrer.migrations` containing a record of each applied migration
  file.

  The invocation of `migrer.core/migrate` can optionally be supplied with a path to the
  migrations and a map of options. Currently only the `:migrer.options/reset` key is
  supported, which if set to a truthy value will rerun all migrations from the first one.
  "
  (:require
   [clojure.java.io :as io]
   [clojure.java.jdbc :as jdbc]
   [clojure.spec.alpha :as s]
   [clojure.string :as str])
  (:import
   [java.security MessageDigest]))

(defn- compare-migration-maps
  [m1 m2]
  (let [{m1-version :migrations/version} m1
        {m2-version :migrations/version} m2]
    (case [(:migrations/type m1) (:migrations/type m2)]
      [:versioned :versioned] (compare m1-version m2-version)
      [:versioned :seed] (let [comparison (compare m1-version m2-version)]
                           (if (= comparison 0) -1 comparison))
      [:versioned :repeatable] -1
      [:seed :seed] (compare m1-version m2-version)
      [:seed :versioned] (let [comparison (compare m1-version m2-version)]
                           (if (= comparison 0) 1 comparison))
      [:seed :repeatable] -1
      [:repeatable :repeatable] (compare m1-version m2-version)
      [:repeatable :versioned] 1
      [:repeatable :seed] 1)))

(def migration-map-comparator (proxy [java.util.Comparator] []
                                (compare [o1 o2]
                                  (compare-migration-maps o1 o2))))

(comment
  (def c (proxy [java.util.Comparator] []
           (compare [o1 o2]
             (compare-migration-maps o1 o2))))

  (sort c [#:migrations{:type :repeatable
                        :version "001"}
           #:migrations{:type :versioned
                        :version "001"}
           #:migrations{:type :seed
                        :version "001"}
           #:migrations{:type :repeatable
                        :version "000"}
           #:migrations{:type :seed
                        :version "000"}
           #:migrations{:type :versioned
                        :version "000"}]))

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
  [root path]
  (let [[filename type version description] (extract-from-path path)]
    (assert (some #{"V" "R" "S"} [type]) (str "Unknown migration type: " type ". Supported types are [\"V\"ersioned \"S\"eed \"R\"epeatable]"))
    (let [migration-type (case type
                           "V" :versioned
                           "S" :seed
                           "R" :repeatable)]
      #:migrations{:type migration-type
                   :filename filename
                   :sql (read-resource (str root path))
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

(defn- read-migration-resources
  "Returns a vector of all migrations in migration root."
  {:impl-doc "Uses getResourceAsStream on the context classloader because the regular
             clojure.java.io/resource call does not handle directories. We use the
             resource stream to read the names of all resources inside a resource dir."}
  [root exclusions repeatable-hashes]
  (with-open [rdr (io/reader
                   (.getResourceAsStream
                    (.. Thread currentThread getContextClassLoader)
                    root))]
    (sort migration-map-comparator
          (into []
                (comp
                 (remove exclusions)
                 (map (partial migration-map root))
                 (remove (fn [{filename :migrations/filename sql :migrations/sql}]
                           (when-let [old-hash (get repeatable-hashes filename)]
                             (= old-hash (md5sum sql))))))
                (line-seq rdr)))))

(defn- exclusions
  [conn]
  (let [performed-migrations (jdbc/query conn ["SELECT filename FROM migrations WHERE type <> 'repeatable';"])]
    (into #{} performed-migrations)))

(defn- repeatable-hashes
  [conn]
  (let [repeatable-hashes (jdbc/query conn ["SELECT filename, hash, performed_at AS \"performed-at\" FROM migrations WHERE type = 'repeatable';"])]
    (into {}
          (map (fn [filename [entry]]
                 [filename entry]))
          (group-by :filename repeatable-hashes))))

(defn- perform-migration-sql
  [conn {sql :migrations/sql :as migration-map} log-fn]
  (log-fn {:event/type :start} migration-map)
  (try
    (log-fn {:event/type :progress :event/data sql})
    (let [time-start (.. (new java.util.Date) (getTime))]
      (jdbc/execute! conn [sql])
      (log-fn {:event/type :done
               :event/data {:ms (- time-start (.. (new java.util.Date) (getTime)))}})
      (let [{type :migrations/type
             version :migrations/version
             filename :migrations/filename} migration-map]
        (if (= type :repeatable)
          (jdbc/execute! conn ["INSERT INTO migrations (type, version, filename, hash) VALUES (?, ?, ?, ?)"
                               type version filename (md5sum sql)])
          (jdbc/execute! conn ["INSERT INTO migrations (type, version, filename) VALUES (?, ?, ?)"
                               type version filename])))
      :result/done)
    (catch Exception e
      (log-fn {:event/type :error :event/data (.getCause e)})
      :result/error)))

(defn- log-migration
  [event migration-map]
  (let [{event-type :type} event
        {type :migrations/type
         version :migrations/version
         sql :migrations/sql
         description :migrations/description} migration-map]
    (case event-type
      (:start) (println (str "=== " event-type " === [" type " | " description " @ " version "]"))
      (:progress) (println (:event/data event))
      (:error) (println (str "=== " event-type " ===\n"
                             (:event/data event)
                             "=== ERROR REPORT END ==="
                             "\n"))
      (:done) (println (str "=== " event-type " ===\n"
                            "=== time: " (get-in event [:event/data :ms]) "ms ==="
                            "\n")))))

(defn init!
  "Initialise migrations metadata on database."
  [conn]
  (jdbc/execute! conn [(jdbc/create-table-ddl :migrations
                                              [[:type "varchar(32)" "NOT NULL"]
                                               [:version "varchar(32)" "NOT NULL"]
                                               [:filename "varchar(256)" "NOT NULL"]
                                               [:hash "varchar(256)"]
                                               [:performed_at "timestamp with time zone" "NOT NULL DEFAULT now()"]]
                                              {:conditional? true})])
  (jdbc/execute! conn ["CREATE INDEX IF NOT EXISTS migrations_type_idx ON migrations (type);"]))

(defn migrate
  "Run any pending migrations."
  ([conn]
   (migrate conn "migrations/"))
  ([conn path]
   (migrate conn path {}))
  ([conn path options]
   (reduce (fn [acc migration-map]
             (if (= (perform-migration-sql conn migration-map (or (:log-fn options) log-migration))
                    :result/error)
               (reduced acc)
               (conj acc migration-map)))
           []
           (read-migration-resources path
                                     (if (get options :migrer/clean?)
                                       #{}
                                       (exclusions conn))
                                     (if (get options :migrer/clean?)
                                       {}
                                       (repeatable-hashes conn))))))
