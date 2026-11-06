#[cfg(feature = "duckdb")]
use crate::duckdb_connector::DuckDBConnection;
use crate::parser::Parser;
use crate::postgres_connector::PostgresConnection;
use crate::qlever_connector::QLeverConnection;
use async_compression::tokio::bufread::GzipDecoder;
use clap::{Arg, ArgAction, ArgGroup, command, value_parser};
use csv::ReaderBuilder;
use futures::TryStreamExt;
use itertools::iproduct;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, create_dir_all, remove_dir_all};
use std::io::Write;
use std::process::Command;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio_util::io::StreamReader;

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
    let matches = setup_cli();

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
        if dbs
            .iter()
            .any(|x| x.name() == "duckdb" || x.name() == "postgres")
        {
            let rt = Runtime::new().unwrap();
            let handle = rt.handle();

            let _ = handle
                .block_on(download_dblp_data("/extern/data/dblp.xml".into()))
                .unwrap();
            let mut parser = Parser::new("/extern/data/dblp.xml");
            parser.run();
            println!("Finished Parsing DBLP data");
        }
    }
    if matches.get_flag("osm-planet") {
        datasets.push(Dataset::OsmPlanet);
        // Download osm-planet data
        if dbs
            .iter()
            .any(|x| x.name() == "duckdb" || x.name() == "postgres")
        {
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
        .map(|(db, dataset)| Test { db, dataset })
        .collect();

    run_tests(
        tests,
        iter,
        matches.get_flag("raw"),
        matches.get_flag("agg"),
        matches.get_flag("hotrun"),
    );
    clean_up();
    println!("Finished Tests");
}

fn setup_cli() -> clap::ArgMatches {
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
            // Require at least one dataset
            ArgGroup::new("dataset")
                .args(["dblp", "osm-planet"])
                .multiple(true),
        )
        .get_matches();
    matches
}

fn run_tests(tests: Vec<Test>, iter: usize, raw: bool, aggregate: bool, hotrun: bool) {
    // Run Tests
    for test in tests {
        clear_cache().expect("Failed to clear cache");
        let name = test.name();
        let query_file = test.dataset.get_query_file();
        println!("Start of Test: {}", name);
        // Create Connection and insert Data
        let mut conn = test
            .db
            .to_connection(test.dataset)
            .expect(format!("Failed to create connection for {}", name).as_str());
        // Run Queries
        let results: Vec<TestResult> = run_test(&query_file, iter, &mut conn, false)
            .expect(format!("Failed while testing coldrun for {}", name).as_str());
        let mut hot_results: Vec<TestResult> = Vec::new();
        if hotrun {
            hot_results = run_test(&query_file, iter, &mut conn, true)
                .expect(format!("Failed while testing hotrun for {}", name).as_str());
        }
        // Save Results
        save_results(&name, raw, aggregate, results, hot_results);
        // Clean Up
        conn.close()
            .expect(format!("Failed to close connection for {}", name).as_str());
        println!("End of Test: {}", name);
    }
}

/// Write the results to their corresponding tsv files
fn save_results(
    name: &String,
    raw: bool,
    aggregate: bool,
    results: Vec<TestResult>,
    hot_results: Vec<TestResult>,
) {
    let _ = create_dir_all("/extern/results");
    // Write raw results to a tsv file
    if raw {
        write_results(
            &results,
            File::create(format!("/extern/results/{}-cold.raw.tsv", name)).unwrap(),
        )
        .expect(
            format!(
                "Failed while writing raw results of cold run {} to file",
                name
            )
            .as_str(),
        );

        if !hot_results.is_empty() {
            write_results(
                &hot_results,
                File::create(format!("/extern/results/{}-hot.raw.tsv", name)).unwrap(),
            )
            .expect(
                format!(
                    "Failed while writing raw results of hot run {} to file",
                    name
                )
                .as_str(),
            );
        }
    }
    // Write aggregated results to a tsv file
    if aggregate {
        write_results_aggregated(
            &results,
            File::create(format!("/extern/results/{}-cold.aggregate.tsv", name)).unwrap(),
        )
        .expect(
            format!(
                "Failed while writing aggregate results of cold run {} to file",
                name
            )
            .as_str(),
        );

        if !hot_results.is_empty() {
            write_results_aggregated(
                &hot_results,
                File::create(format!("/extern/results/{}-hot.aggregate.tsv", name)).unwrap(),
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
}

/// Struct storing info about the Performance Test
pub struct Test {
    db: Database,
    dataset: Dataset,
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
pub enum Dataset {
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
            _ => false,
        }
    }
}

/// Defines Database via [`name`](Database::name) function.
/// Creates a corresponding [`Connection`] with [`to_connection`](Database::to_connection).
#[derive(Clone)]
pub enum Database {
    QLever,
    #[cfg(feature = "duckdb")]
    DuckDB,
    Postgres,
}

impl Database {
    /// Returns name of database
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
    pub fn run_test_query(&mut self, record: &TSVRecord) -> Result<u128, Box<dyn Error>> {
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
            Connection::DuckDB(connection) => connection.cooldown(),
            Connection::PostGres(connection) => connection.cooldown(),
        };
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

#[derive(Serialize)]
/// Contains the results of the performance test.
pub struct TestResult {
    /// Index of the test executed.
    id: usize,
    failures: usize,
    /// Vector of time results in ms.
    results: Vec<u128>,
}

impl TestResult {
    /// Create new TestResult
    pub fn new(id: usize) -> TestResult {
        TestResult {
            id,
            results: Vec::new(),
            failures: 0,
        }
    }

    pub fn add_time(&mut self, time: u128) {
        self.results.push(time);
    }

    pub fn inc_failures(&mut self) -> () {
        self.failures += 1;
    }
}

#[derive(Serialize)]
/// Aggregates test results and stores it
pub struct AggregateTestResult {
    min: String,
    max: String,
    quart_1: String,
    median: String,
    quart_3: String,
    avg: String,
    mode: String,
    failures: String,
}

impl AggregateTestResult {
    pub fn new(raw_result: &TestResult) -> AggregateTestResult {
        let mut numbers = raw_result.results.clone();
        let mut min: String = String::from("-");
        let mut max: String = String::from("-");
        let mut quart_1: String = String::from("-");
        let mut median: String = String::from("-");
        let mut quart_3: String = String::from("-");
        let mut avg: String = String::from("-");
        let mut mode: String = String::from("-");
        if !numbers.is_empty() {
            numbers.sort();
            avg = format!(
                "{:.2}",
                (numbers.iter().sum::<u128>() as f64 / numbers.len() as f64)
            );
            min = numbers.first().unwrap().to_string();
            max = numbers.last().unwrap().to_string();
            quart_1 = format!("{:.2}", (Self::percentile(&numbers, 0.25)));
            median = format!("{:.2}", (Self::percentile(&numbers, 0.5)));
            quart_3 = format!("{:.2}", (Self::percentile(&numbers, 0.75)));
            let mut occurences = HashMap::new();
            mode = numbers
                .iter()
                .copied()
                .max_by_key(|&n| {
                    let count = occurences.entry(n).or_insert(0);
                    *count += 1;
                    *count
                })
                .unwrap()
                .to_string();
        }

        AggregateTestResult {
            min,
            max,
            quart_1,
            median,
            quart_3,
            avg,
            mode,
            failures: raw_result.failures.to_string(),
        }
    }

    // Calculate Percentiles
    fn percentile(sorted: &[u128], p: f64) -> f64 {
        if sorted.len() == 1 {
            return sorted[0] as f64;
        }
        let rank = p * (sorted.len() - 1) as f64;
        let lo = rank.floor() as usize;
        let hi = rank.ceil() as usize;
        let frac = rank - lo as f64;
        // If lo equals hi no in between value has to be calculated
        if lo == hi {
            sorted[lo] as f64
        } else {
            sorted[lo] as f64 * (1.0 - frac) + sorted[hi] as f64 * frac
        }
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
    let mut results: Vec<TestResult> = (0..queries.len()).map(|id| TestResult::new(id)).collect();

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
            match result {
                Ok(time) => {
                    results[id].add_time(time);
                }
                Err(_) => {
                    results[id].inc_failures();
                }
            }
        }
    }

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
        )
        .into());
    }

    Ok(())
}

/// Writes [`TestResult`]s to a tsv file.
fn write_results(
    results: &Vec<TestResult>,
    writer: impl std::io::Write,
) -> Result<(), Box<dyn Error>> {
    let mut tsv_writer = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .flexible(true)
        .from_writer(writer);

    for result in results {
        tsv_writer.serialize(result)?;
    }

    Ok(())
}

/// Aggregates [`TestResult`]s to min, median, mode, avg, max, 1st and 3rd quartile and writes it into a tsv file.
fn write_results_aggregated(
    results: &Vec<TestResult>,
    writer: impl std::io::Write,
) -> Result<(), Box<dyn Error>> {
    let mut tsv_writer = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_writer(writer);

    // Aggregate
    for result in results {
        let aggregate = AggregateTestResult::new(result);
        tsv_writer.serialize(aggregate)?;
    }
    Ok(())
}

fn clean_up() -> () {
    let _ = remove_dir_all("/extern/data/");
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
    use std::io::Read;
    use std::fs::File;
    use tempfile::TempDir;

    use super::*;
    use std::{assert_eq, vec};

    // Unit Tests for TestResult struct
    #[test]
    fn test_test_result_creation() {
        let result = TestResult::new(10);

        assert_eq!(result.id, 10);
        assert_eq!(result.failures, 0);
        assert_eq!(result.results.len(), 0);
    }

    #[test]
    fn test_add_time() {
        let mut result = TestResult::new(1);

        assert_eq!(result.results.len(), 0);

        result.add_time(200);

        assert_eq!(result.results.len(), 1);
        assert_eq!(result.results[0], 200);
    }

    #[test]
    fn test_inc_failures() {
        let mut result = TestResult::new(1);

        assert_eq!(result.failures, 0);

        result.inc_failures();

        assert_eq!(result.failures, 1);
    }

    // Unit Tests for AggregateTestResult struct
    #[test]
    fn test_simple_aggregate_creation() {
        // Setup
        let mut test = TestResult::new(1);
        test.add_time(25);
        test.add_time(25);
        test.add_time(36);
        test.add_time(50);
        test.add_time(65);
        test.add_time(100);
        test.add_time(120);
        let agg = AggregateTestResult::new(&test);
        // Test
        assert_eq!(agg.min, "25");
        assert_eq!(agg.max, "120");
        assert_eq!(agg.quart_1, "30.50");
        assert_eq!(agg.median, "50.00");
        assert_eq!(agg.quart_3, "82.50");
        assert_eq!(agg.avg, "60.14");
        assert_eq!(agg.mode, "25");
        assert_eq!(agg.failures, "0")
    }

    #[test]
    fn test_failures_only_aggregate_creation() {
        let mut test = TestResult::new(1);
        for _ in 0..10 {
            test.inc_failures();
        }
        let agg = AggregateTestResult::new(&test);
        // Test
        assert_eq!(agg.min, "-");
        assert_eq!(agg.max, "-");
        assert_eq!(agg.quart_1, "-");
        assert_eq!(agg.median, "-");
        assert_eq!(agg.quart_3, "-");
        assert_eq!(agg.avg, "-");
        assert_eq!(agg.mode, "-");
        assert_eq!(agg.failures, "10")
    }

    #[test]
    fn test_complex_aggregate_creation() {
        let mut test = TestResult::new(1);
        test.add_time(25);
        test.add_time(25);
        test.add_time(36);
        test.add_time(50);
        test.add_time(65);
        test.add_time(100);
        test.add_time(120);
        for _ in 0..10 {
            test.inc_failures();
        }
        let agg = AggregateTestResult::new(&test);
        // Test
        assert_eq!(agg.min, "25");
        assert_eq!(agg.max, "120");
        assert_eq!(agg.quart_1, "30.50");
        assert_eq!(agg.median, "50.00");
        assert_eq!(agg.quart_3, "82.50");
        assert_eq!(agg.avg, "60.14");
        assert_eq!(agg.mode, "25");
        assert_eq!(agg.failures, "10")
    }

    #[test]
    fn test_write_results() {
        // Setup
        let tmpdir = TempDir::new().unwrap();
        let tmpfile_path = tmpdir.path().join("result.tsv");
        let tmp_file = File::create(tmpfile_path.clone()).unwrap();
        let number_results: Vec<u128> = vec![
            160, 20, 46, 129, 146, 197, 23, 4, 159, 107, 199, 200, 142, 122, 13, 78, 198, 167, 100,
            65,
        ];

        let results: Vec<TestResult> = vec![TestResult {
            id: 1,
            failures: 1,
            results: number_results.clone(),
        }];

        let mut concat: Vec<u128> = vec![1, 1];
        concat.append(number_results.clone().as_mut());
        let concat_string = concat
            .iter()
            .map(|it| it.to_string())
            .collect::<Vec<String>>()
            .join("\t")
            + "\n";

        assert!(write_results(&results, tmp_file).is_ok());

        let mut file = File::open(tmpfile_path).unwrap();
        let mut string = String::new();
        let _ = file.read_to_string(&mut string);
        assert_eq!(string, concat_string);
    }

    #[test]
    fn test_write_results_aggregated() {
        // Setup
        let tmpdir = TempDir::new().unwrap();
        let tmpfile_path = tmpdir.path().join("result.tsv");
        let tmp_file = File::create(tmpfile_path.clone()).unwrap();
        let number_results: Vec<u128> = vec![
            160, 20, 46, 129, 146, 197, 23, 4, 159, 107, 199, 200, 142, 100, 13, 78, 198, 167, 100,
            65,
        ];

        let results: Vec<TestResult> = vec![TestResult {
            id: 1,
            failures: 1,
            results: number_results.clone(),
        }];

        assert!(write_results_aggregated(&results, tmp_file).is_ok());

        let mut file = File::open(tmpfile_path).unwrap();
        let mut string = String::new();
        let _ = file.read_to_string(&mut string);
        let result_string = concat!(
            "min\tmax\tquart_1\tmedian\tquart_3\tavg\tmode\tfailures\n",
            "4\t200\t60.25\t118.00\t161.75\t112.65\t100\t1\n"
        );
        assert_eq!(string, result_string);
    }
}
