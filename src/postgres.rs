use crate::parser::Parser;
use postgres::{Client, NoTls, Row};
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::time::Instant;
use postgres::types::ToSql;

pub struct PostgresConnection {
    client: Client,
}

impl PostgresConnection {

    pub fn new(host: String, user: String) -> Self {
        let client = Client::connect(format!("host={0} user={1}", host, user).as_str(), NoTls).unwrap();
        PostgresConnection {
            client
        }
    }

    pub fn create_tables_dblp(&mut self) {
        let mut file = File::open("create_tables.sql").unwrap();
        let mut query = String::new();
        file.read_to_string(&mut query).unwrap();
        self.client.batch_execute(&query).unwrap();
    }

    pub fn insert_dblp_data(&mut self, file: String) {
        let parser = Parser::new(file.as_ref());
        for record in parser {
            let ops = record.generate_sql_ops();
            for op in ops {
                self.client.execute(&op, &[]).unwrap();
            }
        }
    }

    pub fn run_test_query(&mut self, query: &str, params: &[&(dyn ToSql + Sync)], rows: usize, columns: usize) -> Result<u128, Box<dyn Error>> {
        let now = Instant::now();
        let result : Vec<Row> = self.client.query(query, params)?;
        let duration = now.elapsed().as_millis();
        if result.len() == rows && result.get(0).unwrap().len() == columns {
            return Ok(duration)
        }
        Err("Result doesn't match expected size".into())
    }
}