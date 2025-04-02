# Migration Tool

A lightweight migration tool inspired by famous migration tools like [Flyway](https://flywaydb.org/)
or [Liquibase](https://www.liquibase.com/).

Essentially, it keeps your database up to date with a set of SQL scripts that you ship with the code.

It is supposed to run when the application starts up, which is very useful for integration test
with dockerized database since it creates the schema from scratch at startup.
