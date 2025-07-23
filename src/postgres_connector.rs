use crate::dblp_sql::{Affiliation, AffiliationType, Alias, Author, AuthorWebsite, Editor, Publication, PublicationAuthor, PublicationEditor, Publisher, Reference, Resource, Venue};
use std::collections::HashMap;
use postgres::types::ToSql;
use crate::parser::Parser;
use postgres::{Client, NoTls, Row};
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::thread::sleep;
use std::time::{Duration, Instant};
use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig, PortBinding};
use bollard::query_parameters::CreateContainerOptionsBuilder;
use futures::TryStreamExt;
use tokio::runtime::Runtime;
use crate::dblp_sql::{Data, DataManager};

pub struct PostgresConnection {
    client: Client,
    dataset: String,
    docker_id: String,
}

impl PostgresConnection {

    pub fn new(dataset: &String) -> Result<Self, Box<dyn Error>> {
        let host = if cfg!(target_os = "linux") {
            "172.17.0.1"
        } else {
            "host.docker.internal"
        };
        let conn_str = format!(
            "user=postgres password=password host={} dbname=database",
            host
        );
        // Startup Docker container
        let rt = Runtime::new()?;
        let handle = rt.handle();
        let id = handle.block_on(async {
            let docker = Docker::connect_with_defaults().unwrap();

            docker.create_image(
                Some(
                    bollard::query_parameters::CreateImageOptionsBuilder::default()
                        .from_image("postgres:latest")
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

            let config = ContainerCreateBody {
                image: Some("postgres:latest".into()),
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
            docker.start_container(&id, None::<bollard::query_parameters::StartContainerOptions>).await.expect("Failed to start Docker Container");
            id
        });
        
        // Connect to Postgres DB
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
        
        let mut conn = PostgresConnection { client, dataset: dataset.into(), docker_id: id};
        // TODO add more datasets
        match dataset.as_str() {
            "dblp" => {
                conn.create_tables_dblp();
                conn.insert_dblp_data("./src/data/dblp.xml".into());
            },
            _ => { return Err("dataset could not be resolved for postgres Connection".into())}
        }

        Ok(conn)
    }
    
    pub fn renew_conn(&mut self) {
        let host = if cfg!(target_os = "linux") {
            "172.17.0.1"
        } else {
            "host.docker.internal"
        };
        let conn_str = format!(
            "user=postgres password=password host={} dbname=database",
            host
        );
        // Connect to Postgres DB
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
        self.client = client;
    }

    pub fn create_tables_dblp(&mut self) {
        let mut file = File::open("./src/data/create_tables_dblp.sql").unwrap();
        let mut query = String::new();
        file.read_to_string(&mut query).unwrap();
        self.client.batch_execute(&query).unwrap();
        println!("Finished creating tables DBLP");
    }

    pub fn insert_dblp_data(&mut self, file: String) {
        let mut data_manager = DataManager::new(Parser::new(file.as_ref()));
        
        while let Some(block) = data_manager.next() {
            if self.client.is_closed() {
                self.renew_conn();
            }
            match block[0].value.matcher().as_str() {
                "venue" => {
                    let data: Vec<&Venue> = block.iter()
                        .filter_map(|value| {
                            if let Data::Venue(venue) = &value.value {
                                Some(venue)
                            } else { 
                                None
                            }
                        })
                        .collect();
                    self.batch_insert(
                        "Venues",
                        &["name", "type"],
                        &data,
                        |venue| vec![&venue.name, &venue.venue_type],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                "publication" => {
                    let data: Vec<&Publication> = block.iter()
                        .filter_map(|value| {
                            if let Data::Publication(venue) = &value.value {
                                Some(venue)
                            } else {
                                None
                            }
                        })
                        .collect();
                    // Gather venue_ids
                    let mut venue_names = Vec::new();
                    let mut venue_types = Vec::new();
                    let mut publisher_names = Vec::new();

                    for publi in &data {
                        if let Some(venue_key) = publi.get_venue_key() {
                            venue_names.push(venue_key.key);
                            venue_types.push(venue_key.venue_type);
                        }
                        if let Some(publisher_key) = publi.get_publisher_key() {
                            publisher_names.push(publisher_key.key);
                        }
                    }
                    let rows = self.client.query("SELECT id, name, type FROM Venues WHERE name = ANY($1) AND type = ANY($2)", &[&venue_names, &venue_types])
                        .expect("Failed to execute query");
                    let mut venue_ids = HashMap::new();
                    for row in rows {
                        let key = (row.get::<&str, &str>("name")).to_string() + row.get::<&str, &str>("type") as &str;
                        let value: i32 = row.get("id");
                        venue_ids.insert(key, value);
                    }
                    // Gather publisher ids
                    let rows = self.client.query("SELECT id, name FROM Publishers WHERE name = ANY($1)", &[&publisher_names])
                        .expect("Failed to execute query");
                    let mut publisher_ids = HashMap::new();
                    for row in rows {
                        publisher_ids.insert(row.get::<&str, String>("name"), row.get::<&str, u32>("id"));
                    }
                    // Repackage data
                    let new_data: Vec<(&&Publication, Option<&i32>, Option<&u32>)> = data.iter().map(
                        |x| {
                            let mut venue_id = None;
                            if let Some(id) = &x.venue.clone() {
                                venue_id = venue_ids.get(&id.get_string());
                            }
                            let mut publisher_id = None;
                            if let Some(id) = &x.publisher {
                                publisher_id = publisher_ids.get(&id.key);
                            }
                            (x, venue_id, publisher_id)
                        }
                    ).collect();
                    
                    self.batch_insert(
                        "Publications",
                        &["key", "mdate", "title", "year", "month", "type", "school", "isbn", "pages", "volume", "number", "venue_id", "publisher_id"],
                        &new_data,
                        |value| vec![
                            &value.0.key,
                            &value.0.mdate,
                            &value.0.title,
                            &value.0.year,
                            &value.0.month,
                            &value.0.pub_type,
                            &value.0.school,
                            &value.0.isbn,
                            &value.0.pages,
                            &value.0.volume,
                            &value.0.number,
                            &value.1,
                            &value.2,
                        ],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                "publisher" => {
                    let data: Vec<&Publisher> = block.iter()
                        .filter_map(|value| {
                            if let Data::Publisher(publisher) = &value.value {
                                Some(publisher)
                            } else {
                                None
                            }
                        })
                        .collect();
                    self.batch_insert(
                        "Publishers",
                        &["name"],
                        &data,
                        |value| vec![&value.name],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                "editor" => {
                    let data: Vec<&Editor> = block.iter()
                        .filter_map(|value| {
                            if let Data::Editor(editor) = &value.value {
                                Some(editor)
                            } else {
                                None
                            }
                        })
                        .collect();
                    self.batch_insert(
                        "Editors",
                        &["name"],
                        &data,
                        |value| vec![&value.name],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                "author" => {
                    let data: Vec<&Author> = block.iter()
                        .filter_map(|value| {
                            if let Data::Author(val) = &value.value {
                                Some(val)
                            } else {
                                None
                            }
                        })
                        .collect();
                    self.batch_insert(
                        "Authors",
                        &["name", "id", "mdate"],
                        &data,
                        |value| vec![&value.name, &value.id, &value.mdate],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                "reference"  => {
                    let data: Vec<&Reference> = block.iter()
                        .filter_map(|value| {
                            if let Data::Reference(val) = &value.value {
                                Some(val)
                            } else {
                                None
                            }
                        })
                        .collect();
                    self.batch_insert(
                        "References",
                        &["refrence_type", "origin_pub", "dest_pub"],
                        &data,
                        |value| vec![&value.refrence_type, &value.origin.key, &value.destination.key],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                "resource" => {
                    let data: Vec<&Resource> = block.iter()
                        .filter_map(|value| {
                            if let Data::Resource(val) = &value.value {
                                Some(val)
                            } else {
                                None
                            }
                        })
                        .collect();
                    self.batch_insert(
                        "Resources",
                        &["resource_type", "value", "publication"],
                        &data,
                        |value| vec![&value.resource_type, &value.value, &value.publication.key],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                "publicationEditor" => {
                    let data: Vec<&PublicationEditor> = block.iter()
                        .filter_map(|value| {
                            if let Data::PublicationEditor(val) = &value.value {
                                Some(val)
                            } else {
                                None
                            }
                        })
                        .collect();
                    // Gather editor_ids
                    let editor_names: Vec<String> = data.iter().map(
                        |x| {
                            x.editor.clone().key
                        }
                    ).collect();
                    let mut editor_ids = HashMap::new();
                    for row in self.client.query("SELECT id, name FROM Editors WHERE name = ANY($1)", &[&editor_names]).expect("Failed Query") {
                        editor_ids.insert(row.get::<&str, String>("name"), row.get::<&str, u32>("id"));
                    }
                    
                    let new_data: Vec<(&String, &u32)> = data.iter().map(
                        |x| {
                            (&x.publication.key, editor_ids.get(&x.editor.clone().key).unwrap())
                        }
                    ).collect();
                    
                    self.batch_insert(
                        "PublicationEditors",
                        &["publication_key", "editor_id"],
                        &new_data,
                        |value| vec![&value.0, &value.1],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                "publicationAuthor" => {
                    let data: Vec<&PublicationAuthor> = block.iter()
                        .filter_map(|value| {
                            if let Data::PublicationAuthor(val) = &value.value {
                                Some(val)
                            } else {
                                None
                            }
                        })
                        .collect();
                    // Gather author_ids
                    let author_names: Vec<String> = data.iter().map(
                        |x| {
                            x.author.clone().name
                        }
                    ).collect();
                    let author_ids: Vec<String> = data.iter().map(
                        |x| {
                            x.author.clone().id
                        }
                    ).collect();
                    let mut author_keys = HashMap::new();
                    for row in self.client.query("SELECT key, id, name FROM Authors WHERE name = ANY($1) AND id = ANY($2)", &[&author_names, &author_ids]).expect("Failed Query") {
                        author_keys.insert(row.get::<&str, String>("name") + row.get::<&str, &str>("id"), row.get::<&str, u32>("key"));
                    }

                    let new_data: Vec<(&String, &u32)> = data.iter().map(
                        |x| {
                            (&x.publication.key, author_keys.get(&x.author.to_string()).unwrap())
                        }
                    ).collect();

                    self.batch_insert(
                        "PublicationAuthors",
                        &["publication_key", "author_id"],
                        &new_data,
                        |value| vec![&value.0, &value.1],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                "authorWebsite" => {
                    let data: Vec<&AuthorWebsite> = block.iter()
                        .filter_map(|value| {
                            if let Data::AuthorWebsite(val) = &value.value {
                                Some(val)
                            } else {
                                None
                            }
                        })
                        .collect();
                    // Gather author_ids
                    let author_names: Vec<String> = data.iter().map(
                        |x| {
                            x.author.clone().name
                        }
                    ).collect();
                    let author_ids: Vec<String> = data.iter().map(
                        |x| {
                            x.author.clone().id
                        }
                    ).collect();
                    let mut author_keys = HashMap::new();
                    for row in self.client.query("SELECT key, id, name FROM Authors WHERE name = ANY($1) AND id = ANY($2)", &[&author_names, &author_ids]).expect("Failed Query") {
                        author_keys.insert(row.get::<&str, String>("name") + row.get::<&str, &str>("id"), row.get::<&str, u32>("key"));
                    }

                    let new_data: Vec<(&String, &u32)> = data.iter().map(
                        |x| {
                            (&x.url, author_keys.get(&x.author.to_string()).unwrap())
                        }
                    ).collect();

                    self.batch_insert(
                        "AuthorWebsites",
                        &["url", "author_id"],
                        &new_data,
                        |value| vec![&value.0, &value.1],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                "affiliation" => {
                    let data: Vec<&Affiliation> = block.iter()
                        .filter_map(|value| {
                            if let Data::Affiliation(val) = &value.value {
                                Some(val)
                            } else {
                                None
                            }
                        })
                        .collect();
                    // Gather author_ids
                    let author_names: Vec<String> = data.iter().map(
                        |x| {
                            x.author.clone().name
                        }
                    ).collect();
                    let author_ids: Vec<String> = data.iter().map(
                        |x| {
                            x.author.clone().id
                        }
                    ).collect();
                    let mut author_keys = HashMap::new();
                    for row in self.client.query("SELECT key, id, name FROM Authors WHERE name = ANY($1) AND id = ANY($2)", &[&author_names, &author_ids]).expect("Failed Query") {
                        author_keys.insert(row.get::<&str, String>("name") + row.get::<&str, &str>("id"), row.get::<&str, u32>("key"));
                    }

                    let new_data: Vec<(&String, &AffiliationType, &u32)> = data.iter().map(
                        |x| {
                            (&x.affiliation, &x.aff_type, author_keys.get(&x.author.to_string()).unwrap())
                        }
                    ).collect();

                    self.batch_insert(
                        "AuthorWebsites",
                        &["affiliation", "type", "author_id"],
                        &new_data,
                        |value| vec![&value.0, &value.1, &value.2],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                "alias" => {
                    let data: Vec<&Alias> = block.iter()
                        .filter_map(|value| {
                            if let Data::Alias(val) = &value.value {
                                Some(val)
                            } else {
                                None
                            }
                        })
                        .collect();
                    // Gather author_ids
                    let author_names: Vec<String> = data.iter().map(
                        |x| {
                            x.author.clone().name
                        }
                    ).collect();
                    let author_ids: Vec<String> = data.iter().map(
                        |x| {
                            x.author.clone().id
                        }
                    ).collect();
                    let mut author_keys = HashMap::new();
                    for row in self.client.query("SELECT key, id, name FROM Authors WHERE name = ANY($1) AND id = ANY($2)", &[&author_names, &author_ids]).expect("Failed Query") {
                        author_keys.insert(row.get::<&str, String>("name") + row.get::<&str, &str>("id"), row.get::<&str, u32>("key"));
                    }

                    let new_data: Vec<(&String, &u32)> = data.iter().map(
                        |x| {
                            (&x.alias, author_keys.get(&x.author.to_string()).unwrap())
                        }
                    ).collect();

                    self.batch_insert(
                        "AuthorWebsites",
                        &["alias", "author_id"],
                        &new_data,
                        |value| vec![&value.0, &value.1],
                        Some("ON CONFLICT DO NOTHING")
                    );
                },
                _ => { eprintln!("Invalid Data")}
            }
            data_manager.log();
        }
    }

    fn batch_insert<T, F>(
        &mut self,
        table: &str,
        columns: &[&str],
        items: &[T],
        extractor: F,
        on_conflict: Option<&str>,
    )
    where
        F: Fn(&T) -> Vec<&(dyn ToSql + Sync)>,
    {
        if items.is_empty() {
            return
        }

        let column_list = columns.join(", ");
        let num_columns = columns.len();
        let num_items = items.len();

        // Generate placeholders ($1, $2), ($3, $4), ...
        let placeholders: Vec<String> = (0..num_items)
            .map(|row_index| {
                let start = row_index * num_columns + 1;
                let placeholders: Vec<String> = (0..num_columns)
                    .map(|col_index| format!("${}", start + col_index))
                    .collect();
                format!("({})", placeholders.join(", "))
            })
            .collect();

        // Build query with optional conflict clause
        let mut query = format!(
            "INSERT INTO {} ({}) VALUES {}",
            table,
            column_list,
            placeholders.join(", ")
        );

        if let Some(clause) = on_conflict {
            query.push_str(" ");
            query.push_str(clause);
        }

        // Prepare parameters
        let params: Vec<&(dyn ToSql + Sync)> = items.iter()
            .flat_map(|item| extractor(item))
            .collect();

        let _ = self.client.execute(&query, &params);
    }
    
    pub fn run_test_query(&mut self, query: &str, rows: usize, columns: usize) -> Result<u128, Box<dyn Error>> {
        let now = Instant::now();
        let result : Vec<Row> = self.client.query(query, &[])?;
        let duration = now.elapsed().as_millis();
        if result.len() == rows && result.get(0).unwrap().len() == columns {
            return Ok(duration)
        }
        Err("Result doesn't match expected size".into())
    }
    
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