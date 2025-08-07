#[cfg(feature="duckdb")]
use crate::duckdb_connector::DuckDBConnection;
use crate::postgres_connector::PostgresConnection;
use crate::qlever_connector::QLeverConnection;
use async_compression::tokio::bufread::GzipDecoder;
use clap::{command, value_parser, Arg, ArgAction};
use csv::ReaderBuilder;
use futures::TryStreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::ops::AddAssign;
use std::process::Command;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio_util::io::StreamReader;
use crate::parser::Parser;

mod parser;
#[cfg(feature = "duckdb")]
mod duckdb_connector;
mod postgres_connector;
mod qlever_connector;

const VENUE_FILE: &str = "./src/data/venues.csv";
const PUBLISHER_FILE: &str = "./src/data/publishers.csv";
const EDITOR_FILE: &str = "./src/data/editors.csv";
const AUTHOR_FILE: &str = "./src/data/authors.csv";
const PUBLICATION_FILE: &str = "./src/data/publications.csv";
const RESOURCES_FILE: &str = "./src/data/resources.csv";
const PUBLICATION_EDITOR_FILE: &str = "./src/data/publication_editors.csv";
const REFERENCE_FILE: &str = "./src/data/references.csv";
const PUBLICATION_AUTHORS_FILE: &str = "./src/data/publication_authors.csv";
const AUTHOR_WEBSITES_FILE: &str = "./src/data/authors_website.csv";
const AFFILIATIONS_FILE: &str = "./src/data/affiliations.csv";
const ALIAS_FILE: &str = "./src/data/aliases.csv";

fn main() {
    // CLI Setup
    let matches = command!()
        .arg(
            Arg::new("query_file")
                .value_parser(value_parser!(String))
                .help("path to a query file with the tsv format: (name sql sparql columns rows)")
                .required(true)
        )
        .arg(
            Arg::new("data_set")
                .value_parser(["dblp"])
                .help("dataset to use for this test run")
                .required(true)
        )
        .arg(
            Arg::new("iter")
                .short('i')
                .long("iter")
                .value_parser(value_parser!(usize))
                .default_value("1")
                .help("how often queries are repeated")
                .required(false)
        )
        .arg(
            Arg::new("aggregate")
                .short('a')
                .long("aggregate")
                .action(ArgAction::SetTrue)
                .help("save aggregated results to tsv file")
                .required(false)
        )
        .arg(
            Arg::new("raw")
                .short('r')
                .long("raw")
                .action(ArgAction::SetTrue)
                .help("save raw results to tsv file")
                .required(false)
        )
        .arg(
            Arg::new("qlever")
                .short('q')
                .long("qlever")
                .action(ArgAction::SetTrue)
                .required(false)
            )
        .arg(
            Arg::new("postgres")
                .short('p')
                .long("postgres")
                .action(ArgAction::SetTrue)
                .required(false)
        )
        .arg(
            Arg::new("duckdb")
                .short('d')
                .long("duckdb")
                .action(ArgAction::SetTrue)
                .required(false)
        )
        .get_matches();
    
    let queries = matches.get_one::<String>("query_file").expect("No 'query_file' argument");
    let data_set = matches.get_one::<String>("data_set")
        .expect("data_set is required");
    let iter = matches.get_one::<usize>("iter").unwrap().to_owned();
    
    let mut tests: Vec<Database> = Vec::new();
    if matches.get_flag("qlever") {
        tests.push(Database::QLever);
    }
    if matches.get_flag("postgres") {
        tests.push(Database::Postgres);
    }
    #[cfg(feature="duckdb")]
    if matches.get_flag("duckdb") {
        tests.push(Database::DuckDB);
    }
    
    // TODO add more datasets
    match data_set.as_ref() {
            "dblp" if tests.iter().any(|x| { x.name() == "duckdb" || x.name() == "postgres"}) => {
                let rt = Runtime::new().unwrap();
                let handle = rt.handle();
                
                let _ = handle.block_on(download_dblp_data("./src/data/dblp.xml".into()));
                let mut parser = Parser::new("./src/data/dblp.xml");
                parser.run()
            },
            _ => (),
        };
    // Run Tests
    for test in tests {
        // Create Connection and insert Data
        let mut conn = test.to_connection(&data_set.to_string())
            .expect(format!("Failed to create connection for {}", test.name()).as_str());
        // Run Queries
        let results = run_test(queries, iter, &mut conn)
            .expect(format!("Failed while testing for {}", test.name()).as_str());
        // Save Results
        let _ = create_dir_all("/extern/data");
        if matches.get_flag("raw") {
            write_results(&results, format!("/extern/data/{}.raw.tsv", test.name()))
                .expect(format!("Failed while writing raw results of {} to file", test.name()).as_str());
        }
        if matches.get_flag("aggregate") {
            write_results_aggregated(&results, format!("/extern/data/{}.aggregate.tsv", test.name()))
                .expect(format!("Failed while writing aggregate results of {} to file", test.name()).as_str());
        }
        // Clean Up
        conn.close().expect(format!("Failed to close connection for {}", test.name()).as_str());
        clear_cache().expect("Failed to clear cache");
    }
}

enum Database {
    QLever,
    #[cfg(feature="duckdb")]
    DuckDB,
    Postgres,
}

impl Database {
    pub fn name(&self) -> &str {
        match self {
            Database::QLever => "qlever",
            #[cfg(feature="duckdb")]
            Database::DuckDB => "duckdb",
            Database::Postgres => "postgres",
        }
    }
    
    pub fn to_connection(&self, dataset: &String) -> Result<Connection, Box<dyn Error>> {
        match self {
            Database::QLever =>  Ok(Connection::QLever(QLeverConnection::new(dataset)?)),
            #[cfg(feature="duckdb")]
            Database::DuckDB => Ok(Connection::DuckDB(DuckDBConnection::new(dataset)?)),
            Database::Postgres => Ok(Connection::PostGres(PostgresConnection::new(dataset)?)),
        }
    }
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
    #[cfg(feature="duckdb")]
    DuckDB(DuckDBConnection),
    PostGres(PostgresConnection),
    QLever(QLeverConnection),
}

impl Connection {
    pub fn run_test_query(&mut self, record: &TSVRecord) -> Result<u128, Box<dyn Error>> {
        match self {
            #[cfg(feature="duckdb")]
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
    
    pub fn close(self) -> Result<(), Box<dyn Error>> {
        match self {
            Connection::QLever(connection) => {connection.stop().expect("qlever stop failed");},
            #[cfg(feature="duckdb")]
            Connection::DuckDB(connection) => {connection.close().expect("connection close failed");},
            Connection::PostGres(connection) => { drop(connection);},
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
    pub fn to_tsv_record(&self) -> Vec<String> {
        let mut results: Vec<String> = Vec::new();
        results.push(self.id.to_string());
        results.push(self.failures.to_string());
        results.append(&mut self.results.iter().map(|x| x.to_string()).collect());
        results
    }
}

fn run_test(filename: &String, iterations: usize, connection: &mut Connection) -> Result<Vec<TestResult>, Box<dyn Error>> {
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

fn write_results(results: &Vec<TestResult>, filename: String) -> Result<(), Box<dyn Error>> {
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

fn write_results_aggregated(results: &Vec<TestResult>, filename: String) -> Result<(), Box<dyn Error>> {
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
    
    println!("Download successful");
    Ok(())
}