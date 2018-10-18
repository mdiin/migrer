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
   [clojure.spec.alpha :as s]
   [clojure.string :as str]))

(defmulti compare-migration-maps
  (fn [m1 m2]
    [(:migrations/type m1) (:migrations/type m2)]))

(defmethod compare-migration-maps [:versioned :versioned]
  [{m1-sequence :migrations/sequence#} {m2-sequence :migrations/sequence#}]
  (compare m1-sequence m2-sequence))

(defmethod compare-migration-maps [:versioned :seed]
  [{versioned-sequence :migrations/sequence#} {seed-sequence :migrations/sequence#}]
  (let [comparison (compare versioned-sequence seed-sequence)]
    (if (= comparison 0) -1 comparison)))

(defmethod compare-migration-maps [:versioned :repeatable]
  [_ _]
  -1)

(defmethod compare-migration-maps [:seed :versioned]
  [{seed-sequence :migrations/sequence#} {versioned-sequence :migrations/sequence#}]
  (let [comparison (compare seed-sequence versioned-sequence)]
    (if (= comparison 0) 1 comparison)))

(defmethod compare-migration-maps [:repeatable :versioned]
  [_ _]
  1)

(defmethod compare-migration-maps [:seed :repeatable]
  [_ _]
  -1)

(defmethod compare-migration-maps [:repeatable :seed]
  [_ _]
  1)

(def migration-map-comparator (proxy [java.util.Comparator] []
                                (compare [o1 o2]
                                  (compare-migration-maps o1 o2))))

(comment
  (def c (proxy [java.util.Comparator] []
           (compare [o1 o2]
             (compare-migration-maps o1 o2))))

  (sort c [#:migrations{:type :versioned
                        :sequence# "001"}
           #:migrations{:type :seed
                        :sequence# "000"}
           #:migrations{:type :repeatable
                        :sequence# "000"}
           #:migrations{:type :versioned
                        :sequence# "000"}]))

(defn- read-resource
  [path]
  (slurp (io/resource path)))

(defn- extract-from-filename
  [type-prefix file-name]
  (-> file-name
      (str/split #"/")
      (last)
      (->> (re-matches (re-pattern (str "^" type-prefix "(\\d+)__(.+).sql$")))
           (rest))))

(defmulti migration-map
  "Given a file-name, create a map with sequence and migration type extracted, and
  data slurped."
  (fn migration-map-dispatch [file-name]
    (first file-name)))

(defmethod migration-map "V"
  [file-name]
  (let [[sequence description] (extract-from-filename file-name)]
    #:migrations{:type :versioned
                 :sql (read-resource file-name)
                 :description description
                 :sequence# sequence}))

(defmethod migration-map "S"
  [file-name]
  (let [[sequence description] (extract-from-filename file-name)]
    #:migrations{:type :seed
                 :sql (read-resource file-name)
                 :description description
                 :sequence# sequence}))

(defmethod migration-map "R"
  [file-name]
  (let [[sequence description] (extract-from-filename file-name)]
    #:migrations{:type :repeatable
                 :sql (read-resource file-name)
                 :description description
                 :sequence# sequence}))

(defn- read-migration-resources
  "Returns a vector of all migrations in migration root."
  {:impl-doc "Uses getResourceAsStream on the context classloader because the regular
             clojure.java.io/resource call does not handle directories. We use the
             resource stream to read the names of all resources inside a resource dir."}
  [root exclusions]
  (with-open [rdr (io/reader
                   (.getResourceAsStream
                    (.. Thread currentThread getContextClassLoader)
                    root))]
    (sort migration-map-comparator
          (into []
                (comp
                 (remove exclusions)
                 (map migration-map))
                (line-seq rdr)))))

(defn- perform-migration-sql
  [conn {sql :migrations/sql}]
  [])

(defn migrate
  ([conn]
   (migrate conn "migrations/"))
  ([conn path]
   (migrate conn path {}))
  ([conn path options]
   (if (get options :migrer/reset?)
     (into []
           (map (partial perform-migration-sql conn))
           (read-migration-resources path #{}))
     [])))
