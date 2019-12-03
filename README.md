explore-table-postgres
======================

Quickly get a feel for some postgres table.

Use if you're looking at a yet unfamiliar database and want to know what's actually inside.

For each column in the specified table the script shows its most frequently used values
as well as the smallest and largest value.

```
USAGE:
    explore-table-postgres [OPTIONS] <database> <table>

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -h, --host <host>            database host [default: 127.0.0.1]
    -p, --password <password>    database password [default: ]
        --port <port>            database port [default: 5432]
    -s, --schema <schema>        database schema [default: public]
    -u, --username <username>    database user name [default: postgres]

ARGS:
    <database>    database name to connect to
    <table>       table name to explore

```

