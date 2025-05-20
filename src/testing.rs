use std::error::Error;
use std::ops::Add;
use std::time::Instant;
use csv::ReaderBuilder;
use postgres::Row;
use serde::Deserialize;
use crate::duckdb_connector::DuckDBConnection;
use crate::postgres_connector::PostgresConnection;
use crate::qlever_connector::QLeverConnection;

pub enum QueryLang {
    SQL,
    SPARQL,
}

#[derive(Debug, Deserialize)]
pub struct TSVRecord {
    name: String,
    sql_query: String,
    sparql_query: String,
    columns: usize,
    row: usize,
}

fn read_test_file(filename: &str) -> Result<Vec<TSVRecord>, Box<dyn Error>> {
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_path(filename)
        .expect("Unable to open file");
    let results: Vec<TSVRecord> =  reader.deserialize().collect::<Result<Vec<TSVRecord>, _>>()?;
    Ok(results)
}

pub enum Connection {
    DuckDB(DuckDBConnection),
    PostGres(PostgresConnection),
    QLever(QLeverConnection),
}

impl Connection {
    pub fn run_test_query(&mut self, record: &TSVRecord) -> Result<u128, Box<dyn Error>> {
       match self {
           Connection::DuckDB(connection) => {
               connection.run_test_query(record.sql_query.as_ref(), record.row, record.columns)
           },
           Connection::PostGres(connection) => {
               connection.run_test_query(record.sql_query.as_ref(), record.row, record.columns)
           },
           Connection::QLever(connection) => {
               connection.run_test_query(record.sparql_query.as_ref(), record.row, record.columns)
           }
       }
    }
}

pub struct TestResult {
    id: usize,
    results: Vec<u128>,
    failures: usize,
}

impl TestResult {

    pub fn add_result(&mut self, result: u128) {
        self.results.push(result);
    }

    pub fn register_failure(&mut self) {
        self.failures += 1;
    }
}


pub fn run_test(filename: String, iterations: usize, mut connection: Connection) -> Result<Vec<TestResult>, Box<dyn Error>> {
    let queries = read_test_file(filename.as_str())?;

    let mut results: Vec<TestResult> = Vec::new();
    
    for (id, record) in queries.iter().enumerate() {
        let mut sub_results:Vec<u128> = Vec::new();
        let mut failures = 0;
        for i in 0 .. iterations {
            // setup
            // Clear Cache
            // query
            let result = connection.run_test_query(record);
            // collect result
            match result {
                Ok(value) => { sub_results.push(value);},
                Err(e) => {failures += 1;},
            }
        }
        results.push(TestResult{id, results: sub_results, failures });
    }
    Ok(results)
}