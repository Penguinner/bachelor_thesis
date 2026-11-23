use crate::{
    AFFILIATIONS_FILE, ALIAS_FILE, AUTHOR_FILE, AUTHOR_WEBSITES_FILE, Dataset, EDITOR_FILE,
    PUBLICATION_AUTHORS_FILE, PUBLICATION_EDITOR_FILE, PUBLICATION_FILE, PUBLISHER_FILE,
    REFERENCE_FILE, RESOURCES_FILE, VENUE_FILE,
};
use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig, PortBinding};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, PruneBuildOptions, PruneImagesOptions, PruneVolumesOptions,
    RestartContainerOptions,
};
use futures::TryStreamExt;
use log::{debug, info, trace};
use postgres::{Client, NoTls};
use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File, remove_dir_all};
use std::io::{BufRead, BufReader, Read, Seek, Write};
use std::os::unix::fs::chown;
use std::process::Command;
use std::sync::OnceLock;
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::{format, vec};
use tokio::runtime::Runtime;

/// Connection for PostgreSQL Database
pub struct PostgresConnection {
    client: Client,
    docker_id: String,
    log_path: String,
    log_stream: Option<BufReader<File>>,
    log_pending: String,
}

impl PostgresConnection {
    pub fn new(dataset: &Dataset) -> Result<Self, Box<dyn Error>> {
        // Get dependencies
        dataset.get_sql_dependencies();
        // Startup Docker container
        trace!("Starting postgres container");
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
            let log_path = format!("/extern/data/{}-postgres", dataset.name());

            // Initialize log directory and set permissions so the postgres container can write to it
            match fs::create_dir(log_path.as_str()) {
                Err(e) => {
                    debug!("{}", e);
                    remove_dir_all(log_path.as_str()).unwrap();
                    fs::create_dir(log_path.as_str()).unwrap();
                },
                _ => {}
            }
            chown(log_path.as_str(), Some(999), Some(999)).unwrap();

            let config = ContainerCreateBody {
                image: Some(image.into()),
                env: Some(vec![
                    "POSTGRES_PASSWORD=password".to_string(),
                    "POSTGRES_USER=postgres".to_string(),
                    "POSTGRES_DB=database".to_string(),
                ]),
                cmd: Some(vec![
                    "postgres".to_string(),
                    "-c".to_string(),
                    "logging_collector=on".to_string(),
                    "-c".to_string(),
                    "log_duration=on".to_string(),
                    "-c".to_string(),
                    format!("log_directory={}", log_path).to_string(),
                    "-c".to_string(),
                    "log_filename=postgresql.log".to_string(),
                ]),
                host_config: Some(HostConfig {
                    binds: Some(vec![format!(
                        "/extern/data/{}-postgres:{}",
                        dataset.name(),
                        log_path
                    )]),
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

        // Connect to Postgres DB
        let client = create_client();
        let mut conn = PostgresConnection {
            client,
            docker_id: id,
            log_path: format!("/extern/data/{}-postgres/postgresql.log", dataset.name()),
            log_stream: None,
            log_pending: String::new(),
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
        conn.open_log_stream()?;
        trace!("Finished creating Postgres client");
        Ok(conn)
    }

    fn open_log_stream(&mut self) -> Result<(), Box<dyn Error>> {
        let mut file = File::open(&self.log_path)?;
        file.seek(std::io::SeekFrom::End(0))?;
        self.log_stream = Some(BufReader::new(file));
        self.log_pending.clear();
        Ok(())
    }

    /// Restart Docker container to ensure cold running condition
    pub fn cooldown(&mut self) {
        let rt = Runtime::new().unwrap();
        let handle = rt.handle();
        handle
            .block_on(async {
                let docker = Docker::connect_with_defaults().unwrap();
                docker
                    .restart_container("postgres", Some(RestartContainerOptions::default()))
                    .await
            })
            .unwrap();
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
        info!("status: {}", &osm2pgsql.status);
        info!("stdout:\n{}", String::from_utf8_lossy(&osm2pgsql.stdout));
        info!("stderr:\n{}", String::from_utf8_lossy(&osm2pgsql.stderr));
        trace!("Finished creating tables and inserting data for OSM");
    }

    /// Create all tables necessary for the dblp dataset
    pub fn create_tables_dblp(&mut self) {
        let mut file = File::open("create_tables_dblp.sql").unwrap();
        let mut query = String::new();
        file.read_to_string(&mut query).unwrap();
        self.client.batch_execute(&query).unwrap();
        trace!("Finished creating tables DBLP");
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
        trace!("Inserted DBLP data into Postgres");
    }

    pub fn read_latest_duration(&mut self) -> Result<u128, Box<dyn Error>> {
        const QUIET: Duration = Duration::from_millis(50);
        const DEADLINE: Duration = Duration::from_secs(2);
        let reader = self.log_stream.as_mut().ok_or("log stream not open")?;
        let deadline = Instant::now() + DEADLINE;
        let mut last_match: Option<(u128, Instant)> = None;

        loop {
            let mut chunk = String::new();
            let n = reader.read_line(&mut chunk)?;

            if n > 0 {
                self.log_pending.push_str(&chunk);
                while let Some(nl) = self.log_pending.find('\n') {
                    let line: String = self.log_pending.drain(..=nl).collect();
                    if let Some(ms) = parse_duration_ms(&line) {
                        last_match = Some((ms, Instant::now()));
                    }
                }
            } else {
                if let Some((ms, t)) = last_match {
                    if t.elapsed() >= QUIET || Instant::now() >= deadline {
                        return Ok(ms);
                    }
                } else if Instant::now() >= deadline {
                    return Err("timed out waiting for duration line".into());
                }
                sleep(Duration::from_millis(5));
            }
        }
    }

    /// Run a test query on the PostgreSQL database.
    pub fn run_test_query(&mut self, query: &str) -> Result<u128, Box<dyn Error>> {
        // Recreate client if connection was closed
        if self.client.is_closed() {
            self.client = create_client();
        }
        // Execute Query
        let response = self.client.simple_query(query);
        // Write Result info to log
        let result: Result<u128, Box<dyn Error>> = match response {
            Ok(data) => {
                // Get columns from first row
                let duration = self.read_latest_duration()?;
                let mut columns = 0;
                let mut rows = 0;
                for msg in &data {
                    if let postgres::SimpleQueryMessage::Row(row) = msg {
                        if columns == 0 {
                            columns = row.len()
                        }
                        rows += 1;
                    }
                }
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
        trace!("Stopping postgres container");
        let rt = Runtime::new().unwrap();
        let handle = rt.handle();

        handle.block_on(async {
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

            docker
                .prune_volumes(None::<PruneVolumesOptions>)
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
    let mut retries = 0;
    let max_retries = 10;
    let client: Client;
    loop {
        match Client::connect(&conn_str, NoTls) {
            Ok(cli) => {
                client = cli;
                break;
            }
            Err(_) if retries < max_retries => {
                retries += 1;
                sleep(Duration::from_secs(2));
            }
            Err(e) => {
                panic!("Failed to connect to database: {}", e);
            }
        }
    }
    client
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
