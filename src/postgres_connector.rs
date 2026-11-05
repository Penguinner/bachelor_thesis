use crate::{AFFILIATIONS_FILE, ALIAS_FILE, AUTHOR_FILE, AUTHOR_WEBSITES_FILE, Dataset, EDITOR_FILE, PUBLICATION_AUTHORS_FILE, PUBLICATION_EDITOR_FILE, PUBLICATION_FILE, PUBLISHER_FILE, REFERENCE_FILE, RESOURCES_FILE, VENUE_FILE};
use bollard::models::{ContainerCreateBody, HostConfig, PortBinding};
use bollard::query_parameters::{CreateContainerOptionsBuilder, RestartContainerOptions};
use bollard::Docker;
use futures::TryStreamExt;
use postgres::{Client, NoTls, Row};
use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use std::process::Command;

/// Connection for PostgreSQL Database
pub struct PostgresConnection {
    client: Client,
    dataset: Dataset,
    docker_id: String,
}

impl PostgresConnection {

    pub fn new(dataset: Dataset) -> Result<Self, Box<dyn Error>> {
        // Startup Docker container
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
            docker.create_image(
                Some(
                    bollard::query_parameters::CreateImageOptionsBuilder::default()
                        .from_image(image)
                        .build(),
                ),
                None,
                None,
            )
                .try_collect::<Vec<_>>()
                .await.expect("Failed Creating Docker Image");

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
                    "POSTGRES_DB=database".to_string()
                ]),
                host_config: Some(HostConfig {
                    port_bindings: Some(port_bindings),
                    ..Default::default()
                }),
                exposed_ports: Some( {
                    let mut ports = HashMap::new();
                    ports.insert("5432/tcp".to_string(),HashMap::new());
                    ports
                }),
                ..Default::default()
            };
            
            let options = CreateContainerOptionsBuilder::default().name("postgres").build();
            
            let id = docker.create_container(
                Some(options),
                config,
            )
                .await
                .expect("Failed to create Docker Container")
                .id;
            // Start Container
            docker.start_container(&id, None::<bollard::query_parameters::StartContainerOptions>).await.expect("Failed to start Docker Container");
            id
        });
        
        // Connect to Postgres DB
        let client = create_client();
        let mut conn = PostgresConnection { client, dataset: dataset.clone(), docker_id: id};
        // Additional initialization depending on dataset
        match dataset {
            Dataset::Dblp => {
                conn.create_tables_dblp();
                conn.insert_dblp_data();
            },
            Dataset::OsmPlanet => {
                let _ = conn.client.execute("CREATE EXTENSION postgis;", &[]);
                conn.insert_osm_data();
            }
        }
        println!("Finished creating Postgres client");
        Ok(conn)
    }

    /// Restart Docker container to ensure cold running condition
    pub fn cooldown(&mut self) -> () {
        let docker = Docker::connect_with_defaults().unwrap();
        docker.restart_container("postgres", Some(RestartContainerOptions::default()));
    }

    /// Load OSM planet data to postgres via osm2pgsql
    fn insert_osm_data(&self) {
        let file_path = format!("/extern/data/planet-latest.osm.pbf");
        let osm2pgsql = Command::new("bash")
        .args(["-c", format!("osm2pgsql -c -d database -U postgres -H 172.17.0.1 -P 5432 {file_path}").as_str()])
        .output()
        .unwrap();
        println!("status: {}", &osm2pgsql.status);
        println!("stdout:\n{}", String::from_utf8_lossy(&osm2pgsql.stdout));
        println!("stderr:\n{}", String::from_utf8_lossy(&osm2pgsql.stderr));
        println!("Finished creating tables and inserting data for OSM");
    }

    /// Create all tables necessary for the dblp dataset
    pub fn create_tables_dblp(&mut self) {
        let mut file = File::open("create_tables_dblp.sql").unwrap();
        let mut query = String::new();
        file.read_to_string(&mut query).unwrap();
        self.client.batch_execute(&query).unwrap();
        println!("Finished creating tables DBLP");
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
            ("Alias", ALIAS_FILE)
        ];
        let mut transaction = self.client.transaction().unwrap();
        // Iterate over the tables to add
        for (table, file) in queries.iter() {
            let file = File::open(file).unwrap();
            let mut reader = BufReader::new(file);
            let mut sink = transaction.copy_in(&format!("COPY {} FROM STDIN (FORMAT CSV, DELIMITER E'\\t', HEADER true)", table)).unwrap();
            
            let mut buffer = String::new();
            loop {
                let bytes_read = reader.read_line(&mut buffer).unwrap();
                if bytes_read == 0 {
                    break;
                }
                sink.write_all((&buffer).as_ref()).unwrap();
                buffer.clear();
            }
            sink.finish().unwrap();
        }
        transaction.commit().unwrap();
        println!("Inserted DBLP data into Postgres");
    }
    
    /// Run a test query on the PostgreSQL database.
    pub fn run_test_query(&mut self, query: &str) -> Result<u128, Box<dyn Error>> {
        // Recreate client if connection was closed
        if self.client.is_closed() {
            self.client = create_client();
        }
        // Execute Query
        let stmt = self.client.prepare(query)?;
        let now = Instant::now();
        let response: Result<Vec<Row>, postgres::Error> = self.client.query(&stmt, &[]);
        let duration = now.elapsed().as_millis();
        // Write Result info to log
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(format!("/extern/data/postgres.{}.log", self.dataset.name()))
            .unwrap();
        
        let logmsg: String;
        let result: Result<u128, Box<dyn Error>> = match response {
            Ok(data) => {
                // Get columns from first row
                let mut columns = 0;
                if let Some(row) = data.get(0) {
                    columns = row.len();
                }
                logmsg = format!("Query: {0}\nDuration: {1}\nResult Size: Columns {2} Rows {3}\n", query, duration, columns, data.len());
                Ok(duration)
            },
            Err(e) => {
                logmsg = format!("Query: {0} failed\n{1}", query, e);
                Err(e.into())
            }
        };
        let _ = file.write(logmsg.as_bytes());
        
        result
    }
    
    // Close connection by stopping and removing the container.
    pub fn close(&mut self) -> Result<(), Box<dyn Error>> {
        //Stop docker container
        let rt = Runtime::new().unwrap();
        let handle = rt.handle();

        handle.block_on(async {
            let docker = Docker::connect_with_defaults().unwrap();

            docker.stop_container(
                self.docker_id.as_str(),
                None::<bollard::query_parameters::StopContainerOptions>
            ).await.unwrap();

            docker.remove_container(self.docker_id.as_str(), None::<bollard::query_parameters::RemoveContainerOptions>).await.unwrap();
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
pub fn create_client() -> Client{
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
            },
            Err(_) if  retries < max_retries => {
                retries += 1;
                sleep(Duration::from_secs(2));
            },
            Err(e) => {
                panic!("Failed to connect to database: {}", e);
            }
        }
    }
    client
}