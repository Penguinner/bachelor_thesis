use crate::{
    AFFILIATIONS_FILE, ALIAS_FILE, AUTHOR_FILE, AUTHOR_WEBSITES_FILE, Dataset, EDITOR_FILE,
    PUBLICATION_AUTHORS_FILE, PUBLICATION_EDITOR_FILE, PUBLICATION_FILE, PUBLISHER_FILE,
    REFERENCE_FILE, RESOURCES_FILE, VENUE_FILE,
};
use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig, PortBinding};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, LogsOptionsBuilder, PruneBuildOptions, PruneImagesOptions, RestartContainerOptions,
};
use futures::{StreamExt, TryStreamExt};
use log::{debug, info};
use postgres::{Client, NoTls};
use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::Command;
use std::sync::OnceLock;
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::{eprintln, format, panic, vec};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

/// Connection for PostgreSQL Database
pub struct PostgresConnection {
    client: Client,
    docker_id: String,
    log_rx: mpsc::Receiver<String>,
    rt: Runtime,
}

impl PostgresConnection {
    pub fn new(dataset: &Dataset) -> Result<Self, Box<dyn Error>> {
        // Get dependencies
        dataset.get_sql_dependencies();
        // Startup Docker container
        info!("Starting postgres container");
        let rt = Runtime::new()?;
        let handle = rt.handle();
        let id = handle.block_on(async {
            let docker = Docker::connect_with_defaults().unwrap();
            let mut image = "postgres:latest";

            // Switch image to postgis for spatial
            if dataset.is_geo() {
                image = "postgis/postgis:18-3.6";
            }

            // Create Image
            docker
                .create_image(
                    Some(
                        bollard::query_parameters::CreateImageOptionsBuilder::default()
                            .from_image(image)
                            .build(),
                    ),
                    None,
                    None,
                )
                .try_collect::<Vec<_>>()
                .await
                .expect("Failed Creating Docker Image");

            let mut port_bindings = HashMap::new();
            port_bindings.insert(
                "5432/tcp".to_string(),
                Some(vec![PortBinding {
                    host_ip: Some("0.0.0.0".to_string()),
                    host_port: Some("5432".to_string()),
                }]),
            );

            // Create Container
            let config = ContainerCreateBody {
                image: Some(image.into()),
                env: Some(vec![
                    "POSTGRES_PASSWORD=password".to_string(),
                    "POSTGRES_USER=postgres".to_string(),
                    "POSTGRES_DB=database".to_string(),
                ]),
                cmd: Some(vec![
                    "-c".to_string(),
                    "log_min_duration_statement=0".to_string(),
                ]),
                host_config: Some(HostConfig {
                    port_bindings: Some(port_bindings),
                    ..Default::default()
                }),
                exposed_ports: Some(vec![String::from("5432/tcp")]),
                ..Default::default()
            };

            let options = CreateContainerOptionsBuilder::default()
                .name("postgres")
                .build();

            let id = docker
                .create_container(Some(options), config)
                .await
                .expect("Failed to create Docker Container")
                .id;
            // Start Container
            docker
                .start_container(
                    &id,
                    None::<bollard::query_parameters::StartContainerOptions>,
                )
                .await
                .expect("Failed to start Docker Container");
            id
        });
        // Create log reader
        let (tx, log_rx) = mpsc::channel::<String>(256);
        {
            let container = id.clone();
            rt.spawn(async move {
                let docker = Docker::connect_with_defaults().unwrap();
                let options = Some(
                    LogsOptionsBuilder::new().stdout(true)
                        .follow(true)
                        .tail("0")
                        .stderr(true)
                        .build()
                );

                let mut stream = docker.logs(&container, options);
                let mut buf = String::new();
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(out) => {
                            buf.push_str(&out.to_string());
                            while let Some(nl) = buf.find('\n') {
                                let line: String = buf.drain(..=nl).collect();
                                if tx.send(line).await.is_err() { return; }
                            }
                        },
                        Err(e) => {
                            debug!("log stream error: {e}");
                            eprintln!("log stream error: {e}")
                        }
                    }
                }
            });
        }


        // Connect to Postgres DB
        let client = create_client();
        let mut conn = PostgresConnection {
            client,
            docker_id: id,
            rt,
            log_rx,
        };
        // Additional initialization depending on dataset
        match dataset {
            Dataset::Dblp => {
                conn.create_tables_dblp();
                conn.insert_dblp_data();
            }
            Dataset::OsmPlanet => {
                let _ = conn.client.execute("CREATE EXTENSION postgis;", &[]);
                conn.insert_osm_data();
            }
        }
        info!("Finished creating Postgres client");
        Ok(conn)
    }

    /// Restart Docker container to ensure cold running condition
    pub fn cooldown(&mut self) {
        self.rt.block_on(async {
            let docker = Docker::connect_with_defaults().unwrap();
            docker
                .restart_container(&self.docker_id, Some(RestartContainerOptions::default()))
                .await
                .expect("restart failed")
        });

        self.client = create_client();

        self.log_rx = self.create_log_reader();

        self.flush_logs();
    }

    /// Load OSM planet data to postgres via osm2pgsql
    fn insert_osm_data(&self) {
        let osm2pgsql = Command::new("bash")
            .args([
                "-c",
                "osm2pgsql -c -d database -U postgres -H 172.17.0.1 -P 5432 /extern/data/planet-latest.osm.pbf",
            ])
            .output()
            .unwrap();
        debug!("status: {}", &osm2pgsql.status);
        debug!("stdout:\n{}", String::from_utf8_lossy(&osm2pgsql.stdout));
        debug!("stderr:\n{}", String::from_utf8_lossy(&osm2pgsql.stderr));
        info!("Finished creating tables and inserting data for OSM");
    }

    /// Create all tables necessary for the dblp dataset
    pub fn create_tables_dblp(&mut self) {
        let mut file = File::open("create_tables_dblp.sql").unwrap();
        let mut query = String::new();
        file.read_to_string(&mut query).unwrap();
        self.client.batch_execute(&query).unwrap();
        info!("Finished creating tables DBLP");
    }

    /// Reads the different csv files generated by the parser and reads them into the database via CSV Reader
    pub fn insert_dblp_data(&mut self) {
        let queries = [
            ("Venues", VENUE_FILE),
            ("Publishers", PUBLISHER_FILE),
            ("Editors", EDITOR_FILE),
            ("Authors", AUTHOR_FILE),
            ("Publications", PUBLICATION_FILE),
            ("Resources", RESOURCES_FILE),
            ("PublicationEditors", PUBLICATION_EDITOR_FILE),
            ("Reference", REFERENCE_FILE),
            ("PublicationAuthors", PUBLICATION_AUTHORS_FILE),
            ("AuthorWebsites", AUTHOR_WEBSITES_FILE),
            ("Affiliations", AFFILIATIONS_FILE),
            ("Alias", ALIAS_FILE),
        ];
        let mut transaction = self.client.transaction().unwrap();
        // Iterate over the tables to add
        for (table, file) in queries.iter() {
            let file = File::open(file).unwrap();
            let mut reader = BufReader::new(file);
            let mut sink = transaction
                .copy_in(&format!(
                    "COPY {} FROM STDIN (FORMAT CSV, DELIMITER E'\\t', HEADER true)",
                    table
                ))
                .unwrap();

            let mut buffer = String::new();
            loop {
                let bytes_read = reader.read_line(&mut buffer).unwrap();
                if bytes_read == 0 {
                    break;
                }
                sink.write_all(buffer.as_ref()).unwrap();
                buffer.clear();
            }
            sink.finish().unwrap();
        }
        transaction.commit().unwrap();
        info!("Inserted DBLP data into Postgres");
    }

    fn create_log_reader(&self) -> mpsc::Receiver<String> {
        // Create log reader
        let (tx, log_rx) = mpsc::channel::<String>(256);
        let container = self.docker_id.clone();
        self.rt.spawn(async move {
            let docker = Docker::connect_with_defaults().unwrap();
            let options = Some(
                LogsOptionsBuilder::new().stdout(true)
                    .follow(true)
                    .tail("0")
                    .stderr(true)
                    .build()
            );

            let mut stream = docker.logs(&container, options);
            let mut buf = String::new();
            while let Some(item) = stream.next().await {
                match item {
                    Ok(out) => {
                        buf.push_str(&out.to_string());
                        while let Some(nl) = buf.find('\n') {
                            let line: String = buf.drain(..=nl).collect();
                            if tx.send(line).await.is_err() { return; }
                        }
                    },
                    Err(e) => {
                        debug!("log stream error: {e}");
                        eprintln!("log stream error: {e}")
                    }
                }
            }
        });
        log_rx
    }

    pub fn read_latest_duration(&mut self) -> Result<u128, Box<dyn Error>> {
        const QUIET: Duration = Duration::from_millis(50);
        const DEADLINE: Duration = Duration::from_secs(60);
        let deadline = Instant::now() + DEADLINE;
        let mut last_match: Option<(u128, Instant)> = None;

        loop {
            if let Some((ms, t)) = last_match && t.elapsed() >= QUIET {
                return Ok(ms);
            }
            let now = Instant::now();
            if now >= deadline {
                return last_match.map(|(ms, _) | ms)
                    .ok_or_else(||"timed out waiting for duration line".into())
            }
            let wait = QUIET.min(deadline - now);
            let res = self.rt.block_on(async {
                tokio::time::timeout(wait, self.log_rx.recv()).await
            });
            match res {
                Ok(Some(line)) => {
                    if let Some(ms) = parse_duration_ms(&line) {
                        last_match = Some((ms, Instant::now()));
                    }
                }
                Ok(None) => return Err("log stream ended".into()),
                Err(_elapsed) => {}
            }
        }
    }

    pub fn flush_logs(&mut self) {
        while self.log_rx.try_recv().is_ok() {}
    }

    /// Run a test query on the PostgreSQL database.
    pub fn run_test_query(&mut self, query: &str) -> Result<u128, Box<dyn Error>> {
        self.flush_logs();
        // Execute Query
        let stmt = self.client.prepare(query)?;
        let response = self.client.query(&stmt, &[]);
        // Write Result info to log
        let result: Result<u128, Box<dyn Error>> = match response {
            Ok(data) => {
                // Get columns from first row
                let duration = self.read_latest_duration()?;
                let columns = stmt.columns().len();
                let rows = data.len();
                
                info!(
                    "Query: {0}\nDuration: {1}\nResult Size: Columns {2} Rows {3}\n",
                    query, duration, columns, rows
                );
                Ok(duration)
            }
            Err(e) => {
                info!("Query: {0} failed\n{1}", query, e);
                Err(e.into())
            }
        };
        result
    }

    // Close connection by stopping and removing the container.
    pub fn close(&mut self) -> Result<(), Box<dyn Error>> {
        //Stop docker container
        info!("Stopping postgres container");
        self.rt.block_on(async {
            let docker = Docker::connect_with_defaults().unwrap();

            docker
                .stop_container(
                    self.docker_id.as_str(),
                    None::<bollard::query_parameters::StopContainerOptions>,
                )
                .await
                .unwrap();

            docker
                .remove_container(
                    self.docker_id.as_str(),
                    None::<bollard::query_parameters::RemoveContainerOptions>,
                )
                .await
                .unwrap();
            docker.prune_build(None::<PruneBuildOptions>).await.unwrap();
            docker
                .prune_images(None::<PruneImagesOptions>)
                .await
                .unwrap();
        });

        Ok(())
    }
}

impl Drop for PostgresConnection {
    fn drop(&mut self) {
        self.close().unwrap();
    }
}

/// Create a new client to access the database
pub fn create_client() -> Client {
    let host = if cfg!(target_os = "linux") {
        "172.17.0.1"
    } else {
        "host.docker.internal"
    };
    let conn_str = format!(
        "user=postgres password=password host={} dbname=database",
        host
    );
    // Attempt to connect to Postgres DB
    const DEADLINE : Duration = Duration::from_secs(60);
    let deadline = Instant::now() + DEADLINE;
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match Client::connect(&conn_str, NoTls) {
            Ok(mut cli) => {
                match cli.simple_query("SELECT 1") {
                    Ok(_) => {
                        info!("postgres ready after {attempt} attemps(s)");
                        return cli
                    },
                    Err(e) if is_starting_up(&e) => {
                        info!("postgres still starting up (attempt {attempt})");
                    },
                    Err(e) => {
                        panic!("postgres query failed: {e}");
                    }
                }
            },
            Err(e) => { 
                debug!("{}",e)
            }
        }

        if Instant::now() >= deadline {
            panic!("postgres not ready after {DEADLINE:?} ({attempt} attempts)")
        }
        sleep(Duration::from_millis(200));
    }
}

fn is_starting_up(err: &postgres::Error) -> bool {
    // 57P03 = cannot_connect_now: "the database system is starting up"
    if let Some(code) = err.code().map(|c| c.code()){
        code == "57P03"
    } else {
        false
    }
}

fn parse_duration_ms(line: &str) -> Option<u128> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"duration:\s+(\d+(?:\.\d+)?)\s+ms").unwrap());
    let caps = re.captures(line)?;
    let ms: f64 = caps.get(1)?.as_str().parse().ok()?;
    Some(ms.round() as u128)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_eq;

    #[test]
    fn test_duration_parse() {
        let result = parse_duration_ms("duration: 20.000 ms");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 20u128);

        assert!(parse_duration_ms("Test Test").is_none())
    }
}
