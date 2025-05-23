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
use std::io::{Read, Write};
use std::ops::AddAssign;
use std::path::PathBuf;
use std::process::Command;
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
    // Run Tests
    let mut tests: Vec<Database> = Vec::new();
    if let Some(qlever) = config.qlever {
        tests.push(Database::QLever(qlever));
    }
    if let Some(postgres) = config.postgres {
        tests.push(Database::Postgres(postgres));
    }
    if let Some(duckdb) = config.duck_db {
        tests.push(Database::DuckDB(duckdb));
    }
    
    for test in tests {
        // Create Connection and insert Data
        let mut conn = test.to_connection(&data.to_string(), &data_dir.to_string())
            .expect(format!("Failed to create connection for {}", test.name()).as_str());
        // Run Queries
        let results = run_test(config.queries.to_string(), config.iterations, &mut conn)
            .expect(format!("Failed while testing for {}", test.name()).as_str());
        // Save Results
        if config.raw {
            write_results(&results, format!("{0}{1}.raw.tsv", data_dir, test.name()))
                .expect(format!("Failed while writing raw results of {} to file", test.name()).as_str());
        }
        if config.aggregate {
            write_results(&results, format!("{0}{1}.aggregate.tsv", data_dir, test.name()))
                .expect(format!("Failed while writing aggregate results of {} to file", test.name()).as_str());
        }
        // Clean Up
        conn.close().expect(format!("Failed to close connection for {}", test.name()).as_str());
        clear_cache().expect("Failed to clear cache");
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
    qlever: Option<QLever>,
    postgres: Option<Postgres>,
    duck_db: Option<DuckDb>
}

enum Database {
    QLever(QLever),
    DuckDB(DuckDb),
    Postgres(Postgres),
}

impl Database {
    pub fn name(&self) -> &str {
        match self {
            Database::QLever(_) => "qlever",
            Database::DuckDB(_) => "duckdb",
            Database::Postgres(_) => "postgres",
        }
    }
    
    pub fn to_connection(&self, dataset: &String, data_dir: &String) -> Result<Connection, Box<dyn Error>> {
        match self {
            Database::QLever(qlever) => {
                Ok(Connection::QLever(QLeverConnection::new(qlever.host.to_string(), qlever.qlever_file.to_string())))
            },
            Database::DuckDB(duckdb) => {
                let duck = DuckDBConnection::new(duckdb.path.to_string(), dataset, data_dir)?;
                Ok(Connection::DuckDB(duck))
            },
            Database::Postgres(postgres) => {
                let post = PostgresConnection::new(
                    postgres.host.to_string(),
                    dataset,
                    data_dir,
                )?;
                Ok(Connection::PostGres(post))
            },
        }
    }
}

#[derive(Deserialize)]
struct QLever {
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
    
    pub fn close(&mut self) -> Result<(), Box<dyn Error>> {
        match self {
            Connection::QLever(connection) => {connection.stop().expect("qlever stop failed");},
            Connection::DuckDB(_) => {},
            Connection::PostGres(connection) => {},
        }
        Ok(())
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

pub fn run_test(filename: String, iterations: usize, connection: &mut Connection) -> Result<Vec<TestResult>, Box<dyn Error>> {
    let queries = read_test_file(filename.as_str())?;
    let mut failures: Vec<usize> = Vec::new();
    let mut results: Vec<Vec<u128>> = Vec::new();

    for _ in 0 .. iterations {
        clear_cache().expect("Failed to clear cache");
        // Run Queries
        for (id, record) in queries.iter().enumerate() {
            let result = connection.run_test_query(record);
            match result {
                Ok(value) => results[id].push(value),
                Err(_) => failures[id].add_assign(1)
            }
        }
    }
   let results =  results.iter().enumerate().map(|(index, value) | {
        TestResult {id: index, results: value.clone(), failures: failures[index].clone() }
    }).collect();
    
    Ok(results)
}

fn clear_cache() -> Result<(), Box<dyn Error>> {
    // Clear Cache
    let sync = Command::new("sync").status().expect("Failed running sync");
    if !sync.success() {
        return Err("Failed running sync".into());
    }
    File::create("/proc/sys/vm/drop_caches")?.write_all(b"3\n")?;
    Ok(())
}

pub fn write_results(results: &Vec<TestResult>, filename: String) -> Result<(), Box<dyn Error>> {
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