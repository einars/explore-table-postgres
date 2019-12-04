
use postgres::{Connection, TlsMode, Error};
use clap::{Arg, App};

fn printable(c: Option<String>) -> String {
    let cutoff = 70;
    match c {
        None => "null".to_string(),
        Some(s) => {
            let t = s.trim().replace("\n", "\\n");
            if t.len() > cutoff {
                let out: String = t.chars().take(cutoff).collect();
                out + ".."
            } else {
                if t == "" {
                    "(empty string)".to_string()
                } else {
                    t
                }
            }

        }
    }
}

fn explore_table(c: &Connection, schema: &str, table: &str) -> Result<(), Error> {

    let sql = format!("select count(*) from {}.{}", schema, table);
    for row in &c.query(&sql, &[])? {
        let n: i64 = row.get(0);
        println!("Table:   {}", table);
        println!("Records: {}", n);
    }

    for row in &c.query("
select 
    column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, ordinal_position, udt_name
from information_schema.columns where lower(table_schema)=lower($1) and lower(table_name)=lower($2) order by ordinal_position",
        &[ &schema, &table ])? {

        let column_name: String = row.get(0);
        let data_type: String = row.get(1);
        let character_maximum_length: Option<i32> = row.get(2);
        let numeric_precision: Option<i32> = row.get(3);
        let numeric_scale: Option<i32> = row.get(4);
        let ordinal_position: i32 = row.get(5);
        let udt_name: Option<String> = row.get(6);

        let mut analyze_min_max = true;
        let mut analyze_distinct = true;

        match data_type.as_str() {
            "boolean" => {
                analyze_min_max = false;
                println!("{} {}", column_name, data_type);
            },
            "date" => {
                analyze_distinct = false;
                println!("{} {}", column_name, data_type);
            },
            "timestamp with time zone" => {
                analyze_distinct = false;
                println!("{} {}", column_name, data_type);
            },
            "timestamp without time zone" => {
                analyze_distinct = false;
                println!("{} {}", column_name, data_type);
            },
            "bigint" =>
                println!("{} {}", column_name, data_type),
            "text" =>
                println!("{} {}", column_name, data_type),
            "character varying" => {
                match character_maximum_length {
                    None => println!("{} varchar", column_name),
                    Some(n) => println!("{} varchar({})", column_name, n),
                }
            },
            "integer" =>
                println!("{} integer({}, {})", column_name, numeric_precision.unwrap(), numeric_scale.unwrap()),
            "numeric" =>
                println!("{} numeric({}, {})", column_name, numeric_precision.unwrap(), numeric_scale.unwrap()),
            "USER-DEFINED" => {
                analyze_min_max = false;
                analyze_distinct = false;
                println!("{} user-defined / {}", column_name, udt_name.unwrap_or("???".to_string()));
            }
            _ =>
                println!("{} {} {:?} {:?} {:?}", column_name, data_type, character_maximum_length, numeric_precision, numeric_scale),

        };

        {
            let sql = format!("select pg_catalog.col_description('{}.{}'::regclass::oid, $1)", schema, table);
            let rows = &c.query(&sql, &[&ordinal_position])?;
            let row = rows.get(0);
            let row_comment: Option<String> = row.get(0);
            row_comment.map( |comment| {
                println!("   Comment: {}", comment);
            });
        }



        let n_dist = {
            let sql = format!("select count(distinct({})) from {}.{}", column_name, schema, table);
            let rows = &c.query(&sql, &[])?;
            let row = rows.get(0);
            let n_dist: i64 = row.get(0);
            if n_dist == 0 {
                println!("  All null");
            } else {
                println!("  Distinct: {}", n_dist);
            }
            n_dist
        };

        if n_dist == 0 {
            // all nulls
        } else {

            if analyze_min_max && (n_dist > 10 || ! analyze_distinct) {
                let sql = format!("select min({})::text, max({})::text from {}.{}", column_name, column_name, schema, table);
                for row in &c.query(&sql, &[])? {
                    let min: Option<String> = row.get(0);
                    let max: Option<String> = row.get(1);
                    println!("       Min: {}", printable(min));
                    println!("       Max: {}", printable(max));

                }
            }

            if analyze_distinct {
                let sql = format!("select v::text, c from (select distinct {} as v, count(*) as c from {}.{} group by 1 order by 2 desc, 1 desc limit 10) as preview", column_name, schema, table);
                for row in &c.query(&sql, &[])? {
                    let value: Option<String> = row.get(0);
                    let count: i64 = row.get(1);
                    println!("  {:8}  {}", count, printable(value));
                }

            }
        }

        println!();
    }

    Ok(())


}

fn main() {


    let matches = App::new("explore-table-postgres")
        .author("Einar Lielmanis <einars@spicausis.lv>")
        .about("Shows extended information about postgres table contents")
        .arg(Arg::with_name("database")
             .help("database name to connect to")
             .index(1)
             .takes_value(true)
             .required(true))
        .arg(Arg::with_name("schema")
             .help("database schema")
             .short("s")
             .long("schema")
             .takes_value(true)
             .default_value("public")
             .required(false))
        .arg(Arg::with_name("host")
             .help("database host")
             .short("h")
             .long("host")
             .takes_value(true)
             .default_value("127.0.0.1")
             .required(false))
        .arg(Arg::with_name("username")
             .help("database user name")
             .short("u")
             .long("username")
             .short("U")
             .short("user")
             .default_value("postgres")
             .takes_value(true)
             .required(false))
        .arg(Arg::with_name("password")
             .help("database password")
             .short("p")
             .long("password")
             .default_value("")
             .takes_value(true)
             .required(false))
        .arg(Arg::with_name("port")
             .help("database port")
             .long("port")
             .takes_value(true)
             .default_value("5432")
             .required(false))
        .arg(Arg::with_name("table")
             .help("table name to explore")
             .takes_value(true)
             .required(true))
        .get_matches();

    let conn_str = format!("postgresql://{}:{}@{}:{}/{}",
                           matches.value_of("username").unwrap(),
                           matches.value_of("password").unwrap(),
                           matches.value_of("host").unwrap(),
                           matches.value_of("port").unwrap(),
                           matches.value_of("database").unwrap(),
                           );

    let conn = Connection::connect(conn_str, TlsMode::None).unwrap();
    explore_table(&conn, matches.value_of("schema").unwrap(), matches.value_of("table").unwrap()).unwrap();
}
