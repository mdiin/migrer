# migrer

migrer is a simple and VCS-friendly utility for migrating databases in any JDBC-supported RDBMS.

*Alpha software*: migrer is still to be considered alpha-quality software. I have yet to test it in a project myself. Because of this, I have yet to
release it to Clojars. If you want to try it out you should use the git-deps feature of `deps.edn`.

## Principles

**Simple**: migrer considers SQL the best DSL for a SQL database.

**Powerful**: migrer allows you to do anything supported by your database in your migrations, because it is not limited by a DSL layer on top of SQL.

**VCS-friendly**: migrer supports the concept of *repeatable* migrations, making it easier to see the evolution of e.g. stored procedures and views.

**Small**: The sources are ~200LOC, so getting to know everything about migrer is easy!

## Installation

### deps.edn

Add the following to the dependencies map of your `deps.edn` file:

```
{github-mddin/migrer {:git/url "https://github.com/mdiin/migrer" :sha "<INSERT CURRENT HEAD SHA>"}}
```

## Quickstart

For the impatient among us:

1. Add `resources` to your projects classpath
2. Create the directory `resources/migrations`
3. Create the file `resources/migrations/V001__create_users_table.sql` with the following contents:

```SQL
CREATE TABLE users (id serial NOT NULL, name text);
```

4. In your project REPL run:

```clojure
(require 'migrer.core)
(def conn {...})            ;; JDBC connection map
(migrer.core/init! conn)    ;; Create the migrations metadata table
(migrer.core/migrate! conn) ;; Run any pending migrations
```

5. Enjoy the new `users` table!

## Migrations

migrer has decided on a set of types and a sensible naming scheme for your migrations. Also, migrer eschews the idea of rolling back a migration, because it believes exercising your migrations is a good way of keeping your database healthy.

### Naming scheme

For migrer to help you, it needs you to name your migrations according to use and order of execution.

The naming scheme is `Txxxx__abc_abc_abc.sql`:

- `T`: The type. Any of `V`ersioned, `S`eed, and `R`epeatable
- `xxxx`: The version. Any string of 1 or more digits, e.g. `000`, `1`, a UNIX timestamp, etc.
- `__`: Double underscores are significant! They separate the type and version from the description
- `abc_abc_abc`: The description. Can be anything really, as long as you separate words by underscores
- `.sql`: Migrations are SQL files

### Migration types

**Versioned** migrations contain non-repeatable DDL changes, such as creating tables, altering columns, creating indices; anything not repeatable basically.

**Seed** migrations are similar to versioned migrations in that they contain statements that cannot (or should not) be performed more than once. This allows you to separate what your schema looks like from the rows initially in the tables. Separating versioned and seed migrations lets your different environments have different initial database row sets.

**Repeatable** migrations contain repeatable DDL changes, i.e. statements that will not fail regardless how many times in a row they are executed. This might be things like creating views and stored procedures (using `CREATE OR REPLACE`).

### Wait, no rollbacks?

Indeed; and with good reason!

In a production setting, rolling back a migration is dangerous at best, and you should never change a migration that has already hit production anyway. Should it happen that a bad migration got through your tests and QA, you are probably going to have to figure out how not to lose data regardless of whether you have a rollback migration or not.

During development, re-creating the database whenever a migration not yet in production is changed makes more sense than rolling back followed by migrating again. Using rollbacks, you are exercising your rollbacks as much as your actual migration, which is probably not what you want.

### Execution order

Migrations are performed in version order, with some additions:

- Seed migrations are coupled to a Versioned migration; i.e. there can be no `S001__seed_foobar.sql` without a `V001__create_foobar.sql`
- Seed migrations are performed immediately following the versioned migration of the same version
- Repeatable migrations are always performed last and in the order defined by their versions

It is possible to have multiple seed migrations for the same versioned migration, but the order of execution of the seed migrations is undefined in that case.

## Configuration

Both `migrer.core/init!` and `migrer.core/migrate!` support an optional second argument used for configuration, a map with the keys:

- `:migrer/root`: Where on the classpath should migrer look for migrations? Defaults to `migrations/`
- `:migrer/table-name`: Where should migrer store metadata about the migrations? Defaults to table `migrations`
- `:migrer/log-fn`: How should migrer report migration progress? Defaults to `#'migrer.core/log-migration`

## Authors

* **Matthias Varberg Ingesman** - *Initial work* - [mdiin](https://github.com/mdiin)

See also the list of [contributors](https://github.com/mdiin/migrer/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* [flyway](https://flywaydb.org/) for validating the idea of repeatable migrations
* [pesterhazy's minimalist migration framework](https://gist.github.com/pesterhazy/9f7c0a7a9edd002759779c1732e0ac43) inspiration
* ActiveRecord migrations for being my entry into the world of relational database migrations

