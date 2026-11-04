#[cfg(feature = "duckdb")]
use crate::duckdb_connector::DuckDBConnection;
use crate::parser::Parser;
use crate::postgres_connector::PostgresConnection;
use crate::qlever_connector::QLeverConnection;
use async_compression::tokio::bufread::GzipDecoder;
use clap::{Arg, ArgAction, ArgGroup, command, value_parser};
use csv::ReaderBuilder;
use futures::TryStreamExt;
use serde::Deserialize;
use std::time::Duration;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, create_dir_all};
use std::io::Write;
use std::process::Command;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio_util::io::StreamReader;
use itertools::iproduct;

#[cfg(feature = "duckdb")]
mod duckdb_connector;
mod parser;
mod postgres_connector;
mod qlever_connector;

// Defines location for dblp parsed files.
const VENUE_FILE: &str = "/extern/data/venues.tsv";
const PUBLISHER_FILE: &str = "/extern/data/publishers.tsv";
const EDITOR_FILE: &str = "/extern/data/editors.tsv";
const AUTHOR_FILE: &str = "/extern/data/authors.tsv";
const PUBLICATION_FILE: &str = "/extern/data/publications.tsv";
const RESOURCES_FILE: &str = "/extern/data/resources.tsv";
const PUBLICATION_EDITOR_FILE: &str = "/extern/data/publication_editors.tsv";
const REFERENCE_FILE: &str = "/extern/data/references.tsv";
const PUBLICATION_AUTHORS_FILE: &str = "/extern/data/publication_authors.tsv";
const AUTHOR_WEBSITES_FILE: &str = "/extern/data/authors_website.tsv";
const AFFILIATIONS_FILE: &str = "/extern/data/affiliations.tsv";
const ALIAS_FILE: &str = "/extern/data/aliases.tsv";

fn main() {
    // CLI Setup
    let matches = command!()
        .arg(
            Arg::new("dblp")
                .long("dblp")
                .action(ArgAction::SetTrue)
                .help("run the test over the dblp dataset"),
        )
        .arg(
            Arg::new("osm-planet")
                .long("osm-planet")
                .action(ArgAction::SetTrue)
                .help("run the test over the osm-planet dataset"),
        )
        .arg(
            Arg::new("iter")
                .short('i')
                .long("iter")
                .value_parser(value_parser!(usize))
                .default_value("1")
                .help("how often queries are repeated")
                .required(false),
        )
        .arg(
            Arg::new("hot")
                .long("hot")
                .action(ArgAction::SetTrue)
                .help("also do a hot run")
                .required(false),
        )
        .arg(
            Arg::new("aggregate")
                .short('a')
                .long("aggregate")
                .action(ArgAction::SetTrue)
                .help("save aggregated results to tsv file")
                .required(false),
        )
        .arg(
            Arg::new("raw")
                .short('r')
                .long("raw")
                .action(ArgAction::SetTrue)
                .help("save raw results to tsv file")
                .required(false),
        )
        .arg(
            Arg::new("qlever")
                .short('q')
                .long("qlever")
                .action(ArgAction::SetTrue)
                .required(false),
        )
        .arg(
            Arg::new("postgres")
                .short('p')
                .long("postgres")
                .action(ArgAction::SetTrue)
                .required(false),
        )
        .arg(
            Arg::new("duckdb")
                .short('d')
                .long("duckdb")
                .action(ArgAction::SetTrue)
                .required(false),
        )
        .group(
            ArgGroup::new("dataset")
                .args(["dblp", "osm-planet"])
                .multiple(true)
        )
        .get_matches();
    
    let iter = matches.get_one::<usize>("iter").unwrap().to_owned();
    
    // Register database to be tested
    let mut dbs: Vec<Database> = Vec::new();
    if matches.get_flag("qlever") {
        dbs.push(Database::QLever);
    }
    if matches.get_flag("postgres") {
        dbs.push(Database::Postgres);
    }
    #[cfg(feature = "duckdb")]
    if matches.get_flag("duckdb") {
        dbs.push(Database::DuckDB);
    }

    // Create directory for storing temporary data
    let _ = create_dir_all("/extern/data");

    let mut datasets: Vec<Dataset> = Vec::new();
    if matches.get_flag("dblp") {
        datasets.push(Dataset::Dblp);
        // Download and Parse dblp data for sql use
        if dbs.iter().any(|x| x.name() == "duckdb" || x.name() == "postgres") {
            let rt = Runtime::new().unwrap();
            let handle = rt.handle();

            let _ = handle.block_on(download_dblp_data("/extern/data/dblp.xml".into())).unwrap();
            let mut parser = Parser::new("/extern/data/dblp.xml");
            parser.run();
            println!("Finished Parsing DBLP data");
        }
    }
    if matches.get_flag("osm-planet") {
        datasets.push(Dataset::OsmPlanet);
        // Download osm-planet data
        if dbs.iter().any(|x| x.name() == "duckdb" || x.name() == "postgres") {
            let url = "https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf";
            println!("Downloading {url}");
            let client = reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(900))
                .build()
                .unwrap();
            let response = client.get(url).send().unwrap();
            let file_path = format!("/extern/data/planet-latest.osm.pbf");
            let mut file = File::create(&file_path).unwrap();
            file.write_all(&response.bytes().unwrap()).unwrap();
        }
    }

    // Create cross join between datasets and databases
    let tests: Vec<Test> = iproduct!(dbs, datasets)
        .map(|(db, dataset)| Test {db, dataset})
        .collect();

    // Run Tests
    for test in tests {
        clear_cache().expect("Failed to clear cache");
        let name = test.name();
        let query_file = test.dataset.get_query_file();
        println!("Start of Test: {}", name);
        // Create Connection and insert Data
        let mut conn = test.db
            .to_connection(test.dataset)
            .expect(format!("Failed to create connection for {}", name).as_str());
        // Run Queries
        let results = run_test(&query_file, iter, &mut conn, false)
            .expect(format!("Failed while testing coldrun for {}", name).as_str());
        // Save Results
        let _ = create_dir_all("/extern/results");
        // Write raw results to a tsv file
        if matches.get_flag("raw") {
            write_results(&results, format!("/extern/results/{}.raw.tsv", name)).expect(
                format!(
                    "Failed while writing raw results of cold run {} to file",
                    name
                )
                .as_str(),
            );
        }
        // Write aggregated results to a tsv file
        if matches.get_flag("aggregate") {
            write_results_aggregated(
                &results,
                format!("/extern/results/{}.aggregate.tsv", name),
            )
            .expect(
                format!(
                    "Failed while writing aggregate results of cold run {} to file",
                    name
                )
                .as_str(),
            );
        }
        // If hot flag is present do another run with no cache clearing or cooldown
        if matches.get_flag("hot") {
            let hot_results = run_test(&query_file, iter, &mut conn, true)
                .expect(format!("Failed while testing hotrun for {}", name).as_str());
            // Write hot raw results to a tsv file
            if matches.get_flag("raw") {
                write_results(&hot_results, format!("/extern/results/{}-hot.raw.tsv", name)).expect(
                    format!(
                        "Failed while writing raw results of hot run {} to file",
                        name
                    )
                    .as_str(),
                );
            }
            // Write hot aggregated results to a tsv file
            if matches.get_flag("aggregate") {
                write_results_aggregated(
                    &hot_results,
                    format!("/extern/results/{}-hot.aggregate.tsv", name),
                )
                .expect(
                    format!(
                        "Failed while writing aggregate results of hot run {} to file",
                        name
                    )
                    .as_str(),
                );
            }
        }
        // Clean Up
        conn.close().expect(format!("Failed to close connection for {}", name).as_str());
        println!("End of Test: {}", name);
    }
    println!("Finished Tests");
}

/// Struct storing info about the Performance Test
pub struct Test {
    db: Database,
    dataset: Dataset
}

impl Test {

    /// Returns the name of the test, in the format "{database name}-{dataset name}".
    pub fn name(&self) -> String {
        let name = format!("{}-{}", self.db.name(), self.dataset.name());
        name
    }
}

/// Defines a dataset via the [`name`](Dataset::name) and [`get_query_file`](Dataset::get_query_file) function.
#[derive(Clone)]
enum Dataset {
    Dblp,
    OsmPlanet,
}

impl Dataset {

    /// Returns the corresponding query file to be used for the tests.
    pub fn get_query_file(&self) -> String {
        match self {
            Dataset::Dblp => "dblp.tsv".to_string(),
            Dataset::OsmPlanet => "osm.tsv".to_string(),
        }
    }

    /// Returns name of the Dataset.
    pub fn name(&self) -> String {
        match self {
            Dataset::Dblp => "DBLP".to_string(),
            Dataset::OsmPlanet => "OSM".to_string(),
        }
    }

    /// Returns bool if the Dataset is spatial.
    /// Needed by PostgreSQL and DuckDB for loading spatial modules on start.
    pub fn is_geo(&self) -> bool {
        match self {
            Dataset::OsmPlanet => true,
            _ => false
        }
    }
}

/// Defines Database via [`name`](Database::name) function.
/// Creates a corresponding [`Connection`] with [`to_connection`](Database::to_connection).
#[derive(Clone)]
enum Database {
    QLever,
    #[cfg(feature = "duckdb")]
    DuckDB,
    Postgres,
}

impl Database {

    // Returns name of database
    pub fn name(&self) -> &str {
        match self {
            Database::QLever => "qlever",
            #[cfg(feature = "duckdb")]
            Database::DuckDB => "duckdb",
            Database::Postgres => "postgres",
        }
    }

    /// Returns corresponding [`Connection`] for dataset.
    /// May return an Error if the connection establishment fails.
    pub fn to_connection(&self, dataset: Dataset) -> Result<Connection, Box<dyn Error>> {
        match self {
            Database::QLever => Ok(Connection::QLever(QLeverConnection::new(dataset)?)),
            #[cfg(feature = "duckdb")]
            Database::DuckDB => Ok(Connection::DuckDB(DuckDBConnection::new(dataset)?)),
            Database::Postgres => Ok(Connection::PostGres(PostgresConnection::new(dataset)?)),
        }
    }
}

/// Defines structure of query file, in tsv format, to be read.
#[derive(Debug, Deserialize)]
pub struct TSVRecord {
    name: String,
    postgresql: String,
    #[cfg(feature = "duckdb")]
    duckdb: String,
    sparql: String,
}

/// Reads a tsv file into a [`Vec<TSVRecord>`].
/// Returns an Error if the file doesn't match the format defined by [`TSVRecord`].
fn read_test_file(filename: &str) -> Result<Vec<TSVRecord>, Box<dyn Error>> {
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_path(filename)
        .expect("Unable to open file");
    let results: Vec<TSVRecord> = reader
        .deserialize()
        .collect::<Result<Vec<TSVRecord>, _>>()?;
    Ok(results)
}

/// Intermediate layer to unify different kind of Connections.
/// Connections: [`DuckDBConnection`], [`PostgresConnection`], [`QLeverConnection`]
pub enum Connection {
    #[cfg(feature = "duckdb")]
    DuckDB(DuckDBConnection),
    PostGres(PostgresConnection),
    QLever(QLeverConnection),
}

impl Connection {

    /// Runs a test for a given [`TSVRecord`] expressing a query.
    /// Passes test query parameters to connection.
    pub fn run_test_query(&mut self, record: &TSVRecord) -> u128 {
        match self {
            #[cfg(feature = "duckdb")]
            Connection::DuckDB(connection) => connection.run_test_query(record.duckdb.as_ref()),
            Connection::PostGres(connection) => {
                connection.run_test_query(record.postgresql.as_ref())
            }
            Connection::QLever(connection) => connection.run_test_query(record.sparql.as_ref()),
        }
    }

    /// Resets connection in different ways to ensure a cold run.
    /// Passes cooldown paramaters to connection.
    pub fn cooldown(&mut self) -> () {
        return match self {
            Connection::QLever(connection) => {
                connection.cooldown();
            }
            #[cfg(feature = "duckdb")]
            Connection::DuckDB(connection) => {
                connection.cooldown()
            }
            Connection::PostGres(connection) => {
                connection.cooldown()
            }
        }
    }

    /// Closes the connection.
    pub fn close(self) -> Result<(), Box<dyn Error>> {
        match self {
            Connection::QLever(connection) => {
                connection.stop().expect("qlever stop failed");
            }
            #[cfg(feature = "duckdb")]
            Connection::DuckDB(connection) => {
                connection.close().expect("connection close failed");
            }
            Connection::PostGres(connection) => {
                drop(connection);
            }
        }
        Ok(())
    }
}

/// Contains the results of the performance test.
pub struct TestResult {
    /// Index of the test executed.
    id: usize,
    /// Vector of time results in ms.
    results: Vec<u128>,
}

impl TestResult {

    /// Converts itself into a Vector of Strings
    pub fn to_string_list(&self) -> Vec<String> {
        let mut results: Vec<String> = Vec::new();
        results.push(self.id.to_string());
        results.append(&mut self.results.iter().map(|x| x.to_string()).collect());
        results
    }
}

/// Runs a full test for a [`Connection`].
/// * `filename` - Path to the query file.
/// * `iterations` - How often the set of queries should be executed.
/// * `connection` - Connection to which the queries should be send.
/// * `hotrun` - Whether it is a hotrun, where no cooldown or cache reset is used and .
/// Returns a [`Vec`]<[`TestResult`]>
fn run_test(
    filename: &String,
    iterations: usize,
    connection: &mut Connection,
    hotrun: bool,
) -> Result<Vec<TestResult>, Box<dyn Error>> {
    let queries = read_test_file(filename.as_str())?;
    // Initialize 2D Vector for results
    let mut results: Vec<Vec<u128>> = vec![Vec::new(); queries.len()];

    if hotrun {
        // Warm up the database
        for _ in 0..5 {
            for (_, record) in queries.iter().enumerate() {
                let _ = connection.run_test_query(record);
            }
        }
    }

    for _ in 0..iterations {
        // Run Queries
        for (id, record) in queries.iter().enumerate() {
            if !hotrun {
                connection.cooldown();
                clear_cache().unwrap();
            }
            let result = connection.run_test_query(record);
            results[id].push(result)
        }
    }
    // Covert results to TestResult
    let results = results
        .iter()
        .enumerate()
        .map(|(index, value)| TestResult {
            id: index,
            results: value.clone(),
        })
        .collect();

    Ok(results)
}

/// Clears OS Cache by executing "sync; echo 3 > /proc/sys/vm/drop_caches".
fn clear_cache() -> Result<(), Box<dyn Error>> {
    // Clear Cache
    let out = Command::new("bash")
        .arg("-c")
        .arg("sync; echo 3 > /drop_caches")
        .output()?;

    if !out.status.success() {
        return Err(format!(
            "drop_caches failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ).into());
    }

    Ok(())
}

/// Writes [`TestResult`]s to a tsv file.
fn write_results(results: &Vec<TestResult>, filename: String) -> Result<(), Box<dyn Error>> {
    let mut writer = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_writer(File::create(filename.as_str())?);

    for result in results {
        writer.write_record(result.to_string_list())?;
    }

    Ok(())
}

/// Aggregates [`TestResult`]s to min, median, mode, avg, max, 1st and 3rd quartile and writes it into a tsv file.
fn write_results_aggregated(
    results: &Vec<TestResult>,
    filename: String,
) -> Result<(), Box<dyn Error>> {
    let mut writer = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_writer(File::create(filename.as_str())?);

    // Write header
    writer.write_record(&["id", "min", "1st-quartile", "median", "3rd-quartile", "mode", "avg", "max"])?;
    // Aggregate
    for result in results {
        let mut numbers = result.results.clone();
        numbers.sort();
        let avg: f64 = numbers.iter().sum::<u128>() as f64 / numbers.len() as f64;
        let min = numbers.first().unwrap();
        let max = numbers.last().unwrap();
        let quart_1 = numbers[numbers.len()/4];
        let median = numbers[numbers.len() / 2];
        let quart_3 = numbers[numbers.len()*3/4];
        let mut occurences = HashMap::new();
        let mode = numbers
            .iter()
            .copied()
            .max_by_key(|&n| {
                let count = occurences.entry(n).or_insert(0);
                *count += 1;
                *count
            })
            .unwrap();
        writer.write_record(&[
            result.id.to_string(),
            min.to_string(),
            quart_1.to_string(),
            median.to_string(),
            quart_3.to_string(),
            mode.to_string(),
            avg.to_string(),
            max.to_string(),
        ])?;
    }

    Ok(())
}

/// Downloads the xml GZIP archive for the dblp dataset.
async fn download_dblp_data(filename: String) -> Result<(), Box<dyn Error>> {
    let url = "https://dblp.org/xml/dblp.xml.gz";

    let client = reqwest::Client::new();
    let response = client.get(url).send().await?;

    if !response.status().is_success() {
        return Err(format!(
            "HTTP request failed with status: {}",
            response.status().as_str()
        )
        .into());
    }

    let stream = response
        .bytes_stream()
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

#[cfg(test)]
mod tests {
    use super::*;


}