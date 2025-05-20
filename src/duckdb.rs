use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::time::Instant;
use duckdb::{params, Connection, ToSql};
use crate::parser::Parser;

pub struct DuckDBConnection {
    connection: Connection,
}

impl DuckDBConnection {
    pub fn new(path: String) -> DuckDBConnection {
        DuckDBConnection {
            connection: Connection::open(path).unwrap(),
        }
    }

    pub fn create_tables_dblp(&mut self) {
        let mut file = File::open("create_tables.sql").unwrap();
        let mut query = String::new();
        file.read_to_string(&mut query).unwrap();
        query = format!("BEGIN;\n {}\n COMMIT;", query);
        self.connection.execute_batch(&query).unwrap();
    }

    pub fn insert_dblp_data(&mut self, file: String) {
        let parser = Parser::new(file.as_ref());
        for record in parser {
            let ops = record.generate_sql_ops();
            for op in ops {
                self.connection.execute(op.as_ref(),params![]).unwrap();
            }
        }
    }

    pub fn run_test_query(&self, query: &str, params: &[&dyn ToSql], rows: usize, columns: usize) -> Result<u128, Box<dyn Error>> {
        let mut stmt = self.connection.prepare(query)?;
        let now = Instant::now();
        let result = stmt.query(params).unwrap();
        let duration = now.elapsed().as_millis();
        if stmt.row_count() == rows && stmt.column_count() == columns {
            return Ok(duration)
        }
        Err("Result doesn't match expected size")?
    }
}