use crate::duckdb_connector::DuckDBConnection;
use crate::postgres_connector::PostgresConnection;
use crate::qlever_connector::QLeverConnection;
use async_compression::tokio::bufread::GzipDecoder;
use clap::{arg, command, value_parser};
use csv::ReaderBuilder;
use futures::TryStreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio_util::io::StreamReader;

mod parser;
mod duckdb_connector;
mod postgres_connector;
mod qlever_connector;

fn main() {
    let matches = command!()
        .arg(arg!(-t --test_file <FILE> "Sets test file.")
                 .required(false)
                 .value_parser(value_parser!(PathBuf)),
        )
        .get_matches();
    let test_file: PathBuf= matches.get_one::<PathBuf>("-t").expect("Couldn't read path").into();
    let mut data = String::new();
    let _ = File::open(test_file).unwrap().read_to_string(&mut data);
    let config = toml::from_str::<Config>(data.as_str()).unwrap();
    let data_dir = if config.data_directory.is_some() {config.data_directory.as_ref().unwrap().as_str()} else {"./"};
    match config.dataset.as_ref() {
            "dblp" => {
                let rt = Runtime::new().unwrap();
                let handle = rt.handle();
                
                let _ = handle.block_on(download_dblp_data(format!("{data_dir}dblp.xml")));
            },
            _ => (),
        };
    // Run Qlever Tests
    if let Some(qlever) = config.qlever {
        // Start qlever with qlever control

        // Create Qlever Connection
        let connection: Connection = Connection::QLever(QLeverConnection::new());
        // Run Queries
        let results = run_test(config.queries.to_string(),config.iterations,connection)
            .expect("Failed while testing qlever");
        // Save Results
        if config.raw {
            write_results(&results, format!("{0}{1}", data_dir, "qlever.raw.tsv"))
                .expect("Failed while writing raw results of qlever to file");
        }
        if config.aggregate {
            write_results(&results, format!("{0}{1}", data_dir, "qlever.aggregate.tsv"))
                .expect("Failed while writing aggregate results of qlever to file");
        }
        // Clean Up
    }
    // Run Postgres Tests
    if let Some(postgres) = config.postgres {
        // Start Postgres Container
        
        // Create Postgres Connection
        let mut connection: Connection = Connection::PostGres(PostgresConnection::new(postgres.host, "postgres".to_string()));
        // Intialize Database
        connection.insert_dblp_data(format!("{data_dir}dblp.xml"));
        // Run Queries
        let results = run_test(config.queries.to_string(), config.iterations, connection).expect("Failed while testing Postgres");
        // Save Results
        if config.raw {
            write_results(&results, format!("{0}{1}", data_dir, "postgres.raw.tsv"))
                .expect("Failed while writing raw results of postgres to file");
        }
        if config.aggregate {
            write_results(&results, format!("{0}{1}", data_dir, "postgres.aggregate.tsv"))
            .expect("Failed while writing aggregate results of postgres to file");
        }
        // Clean Up
    }
    // Run DuckDB Tests
    if let Some(duckdb) = config.duck_db {
        // Create Duckdb Connection
        let mut connection: Connection = Connection::DuckDB(DuckDBConnection::new(duckdb.path));
        // Intialize Database
        connection.insert_dblp_data(format!("{data_dir}dblp.xml"));
        // Run Queries
        let results = run_test(config.queries.to_string(), config.iterations, connection).expect("Failed while testing DuckDB");
        // Save Results
        if config.raw {
            write_results(&results, format!("{0}{1}", data_dir, "duckdb.raw.tsv"))
                .expect("Failed while writing raw results of duckdb to file");
        }
        if config.aggregate {
            write_results(&results, format!("{0}{1}", data_dir, "duckdb.aggregate.tsv"))
                .expect("Failed while writing aggregate results of duckdb to file");
        }
        // Clean Up
    }
}

#[derive(Deserialize)]
struct Config {
    iterations: usize,
    aggregate: bool,
    raw: bool,
    data_directory: Option<String>,
    dataset: String,
    queries: String,
    qlever: Option<Qlever>,
    postgres: Option<Postgres>,
    duck_db: Option<DuckDb>
}

#[derive(Deserialize)]
struct Qlever {
    host : String,
    qlever_file: String,
}

#[derive(Deserialize)]
struct Postgres {
    host : String,
}

#[derive(Deserialize)]
struct DuckDb {
    path: String,
}

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

    pub fn insert_dblp_data(&mut self, file: String) {
        match self {
            Connection::DuckDB(connection) => {
                connection.insert_dblp_data(file);
            },
            Connection::PostGres(connection) => {
                connection.insert_dblp_data(file);
            },
            _ => ()
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

    pub fn to_tsv_record(&self) -> Vec<String> {
        let mut results: Vec<String> = Vec::new();
        results.push(self.id.to_string());
        results.push(self.failures.to_string());
        results.append(&mut self.results.iter().map(|x| x.to_string()).collect());
        results
    }
}

pub fn run_test(filename: String, iterations: usize, mut connection: Connection) -> Result<Vec<TestResult>, Box<dyn Error>> {
    let queries = read_test_file(filename.as_str())?;

    let mut results: Vec<TestResult> = Vec::new();

    for (id, record) in queries.iter().enumerate() {
        let mut sub_results:Vec<u128> = Vec::new();
        let mut failures = 0;
        for _ in 0 .. iterations {
            // setup
            // Clear Cache

            // query
            let result = connection.run_test_query(record);
            // collect result
            match result {
                Ok(value) => { sub_results.push(value);},
                Err(_) => {failures += 1;},
            }
        }
        results.push(TestResult{id, results: sub_results, failures });
    }
    Ok(results)
}

pub fn write_results(results: &Vec<TestResult>, filename: String) -> Result<(), Box<dyn Error>> {
    /*
        id\tfailures\tvalue\tvalue
    */
    let mut writer = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_writer(File::create(filename.as_str())?);

    writer.write_record(&["id", "failures", "values..."])?;

    for result in results {
        writer.write_record(result.to_tsv_record())?;
    }

    Ok(())
}

pub fn write_results_aggregated(results: &Vec<TestResult>, filename: String) -> Result<(), Box<dyn Error>> {
    let mut writer = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_writer(File::create(filename.as_str())?);

    writer.write_record(&["id", "min", "median", "mode", "avg", "max"])?;
    // Aggregate
    for result in results {
        let mut numbers = result.results.clone();
        numbers.sort();
        let avg: f64 = numbers.iter().sum::<u128>() as f64 / numbers.len() as f64;
        let min = numbers.first().unwrap();
        let max = numbers.last().unwrap();
        let median = numbers[numbers.len() / 2];
        let mut occurences = HashMap::new();
        let mode = numbers.iter().copied().max_by_key(|&n| {
            let count = occurences.entry(n).or_insert(0);
            *count += 1;
            *count
        }).unwrap();
        writer.write_record(&[
            result.id.to_string(),
            min.to_string(),
            median.to_string(),
            mode.to_string(),
            avg.to_string(),
            max.to_string(),
        ])?;
    }

    Ok(())
}

async fn download_dblp_data(filename: String) -> Result<(), Box<dyn Error>> {
    let url = "https://dblp.org/xml/dblp.xml.gz";
    
    let client = reqwest::Client::new();
    let response = client.get(url).send().await?;
    
    if !response.status().is_success() {
        return Err(format!("HTTP request failed with status: {}", response.status().as_str()).into());
    }
    
    let stream = response.bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    
    let reader = StreamReader::new(stream);
    
    let mut decoder = GzipDecoder::new(reader);
    
    let mut output_file = tokio::fs::File::create(filename).await?;
    
    let mut buffer = Vec::new();
    decoder.read_to_end(&mut buffer).await?;
    output_file.write_all(&buffer).await?;
    
    Ok(())
}