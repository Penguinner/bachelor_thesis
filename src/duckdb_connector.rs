use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::time::Instant;
use duckdb::{params, params_from_iter, Connection};
use crate::parser::Parser;

pub struct DuckDBConnection {
    connection: Connection,
    dataset: String,
}

impl DuckDBConnection {
    pub fn new(dataset: &String) -> Result<DuckDBConnection,  Box<dyn Error >> {
        let mut conn = DuckDBConnection { connection: Connection::open("./src/data/db.duckdb").unwrap(), dataset: dataset .to_string() };
        // TODO Add more datasets
        match dataset.as_str() {
            "dblp" => {
                conn.create_tables_dblp();
                conn.insert_dblp_data("./src/data/dblp.xml".to_string());
            },
            _ => { return Err("dataset could not be resolved for duckdb Connection".into())}
        }

        Ok(conn)
    }

    pub fn create_tables_dblp(&mut self) {
        let mut file = File::open("./src/data/create_tables_dblp.sql").unwrap();
        let mut query = String::new();
        file.read_to_string(&mut query).unwrap();
        query = format!("BEGIN;\n {}\n COMMIT;", query);
        self.connection.execute_batch(&query).unwrap();
        println!("Created Tables DBLP");
    }

    pub fn insert_dblp_data(&mut self, file: String) {
        let parser = Parser::new(file.as_ref());
        let mut ref_ops = Vec::new();
        let mut record_count = 0;
        for record in parser{
            let ops = record.generate_sql_ops('?');
            print!("record:{0:0>7}\r", record_count);
            ref_ops.append(&mut ops.1.clone());
            for (i, op) in ops.0.iter().enumerate() {
                let mut stmt = self.connection.prepare_cached(op.0.as_ref()).unwrap();
                stmt.execute(params_from_iter(op.1.clone()))
                    .unwrap_or_else(|error| {
                        !panic!("{0} while executing insert query: {1} {2:?}", error, op.0, op.1);
                    });
            }
            record_count += 1;
            print!("record:{0:0>7}\r", record_count);
        }
        print!("\nref_ops:{0:0>5}/{1:0>5}\r", 0, ref_ops.len());
        for (i, op) in ref_ops.iter().enumerate() {
            let mut stmt = self.connection.prepare_cached(op.0.as_ref()).unwrap();
            stmt.execute(params_from_iter(op.1.clone()))
                .unwrap_or_else(|error| {
                    !panic!("{0} while executing insert query: {1} {2:?}", error, op.0, op.1);
                });
            print!("ref_ops:{0:0>5}/{1:0>5}\r", i + 1, ref_ops.len());
        }
    }

    pub fn run_test_query(&self, query: &str, rows: usize, columns: usize) -> Result<u128, Box<dyn Error>> {
        let mut stmt = self.connection.prepare(query)?;
        let now = Instant::now();
        let _ = stmt.query(params![]).unwrap();
        let duration = now.elapsed().as_millis();
        if stmt.row_count() == rows && stmt.column_count() == columns {
            return Ok(duration)
        }
        Err("Result doesn't match expected size")?
    }
    
    pub fn close(self) -> Result<(), Box<dyn Error>> {
        // TODO Clear Data
        self.connection.close().expect("connection close failed");
        Ok(())
    }
}