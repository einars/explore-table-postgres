extern crate postgres;

use postgres::{Connection, TlsMode, Error};

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
    column_name, data_type, character_maximum_length, numeric_precision, numeric_precision_radix, ordinal_position
from information_schema.columns where table_schema=$1 and table_name=$2",
        &[ &schema, &table ])? {

        let column_name: String = row.get(0);
        let data_type: String = row.get(1);
        let character_maximum_length: Option<i32> = row.get(2);
        let numeric_precision: Option<i32> = row.get(3);
        let numeric_precision_radix: Option<i32> = row.get(4);
        let ordinal_position: i32 = row.get(5);

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
            "character varying" => {
                match character_maximum_length {
                    None => println!("{} text", column_name),
                    Some(n) => println!("{} text({})", column_name, n),
                }
            },
            "integer" =>
                println!("{} integer({}, {})", column_name, numeric_precision.unwrap(), numeric_precision_radix.unwrap()),
            "numeric" =>
                println!("{} numeric({}, {})", column_name, numeric_precision.unwrap(), numeric_precision_radix.unwrap()),
            "USER-DEFINED" => {
                analyze_min_max = false;
                analyze_distinct = false;
                println!("{} user-defined", column_name);
            }
            _ =>
                println!("{} {} {:?} {:?} {:?}", column_name, data_type, character_maximum_length, numeric_precision, numeric_precision_radix),

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
                let sql = format!("select distinct {}::text, count(*) from {}.{} group by 1 order by 2 desc limit 10", column_name, schema, table);
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
    let conn_str = "postgresql://postgres@127.0.0.1:5432/izraktenis";
    let conn = Connection::connect(conn_str, TlsMode::None).unwrap();
    explore_table(&conn, "public", "atradne").unwrap();
}
