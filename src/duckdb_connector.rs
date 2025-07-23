use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::time::Instant;
use duckdb::{appender_params_from_iter, params, params_from_iter, AppenderParams, Connection, ToSql};
use duckdb::types::Value::Array;
use crate::dblp_sql::{Affiliation, AffiliationType, Alias, Author, AuthorWebsite, Data, DataManager, Editor, Publication, PublicationAuthor, PublicationEditor, Publisher, Reference, Resource, Venue};
use crate::parser::Parser;

pub struct DuckDBConnection {
    connection: Connection,
    dataset: String,
}

impl DuckDBConnection {
    pub fn new(dataset: &String) -> Result<DuckDBConnection,  Box<dyn Error >> {
        let mut conn = DuckDBConnection { connection: Connection::open("./src/data/db.duckdb").unwrap(), dataset: dataset .to_string() };
        // TODO Add more datasets
        match dataset.as_str() {
            "dblp" => {
                conn.create_tables_dblp();
                conn.insert_dblp_data("./src/data/dblp.xml".to_string());
            },
            _ => { return Err("dataset could not be resolved for duckdb Connection".into())}
        }

        Ok(conn)
    }

    pub fn create_tables_dblp(&mut self) {
        let mut file = File::open("./src/data/create_tables_dblp.sql").unwrap();
        let mut query = String::new();
        file.read_to_string(&mut query).unwrap();
        query = format!("BEGIN;\n {}\n COMMIT;", query);
        self.connection.execute_batch(&query).unwrap();
        println!("Created Tables DBLP");
    }

    pub fn insert_dblp_data(&mut self, file: String) {
        let mut data_manager = DataManager::new(Parser::new(file.as_ref()));

        while let Some(block) = data_manager.next() {
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
                            venue_names.push(duckdb::types::Value::Text(venue_key.key));
                            venue_types.push(duckdb::types::Value::Enum(venue_key.venue_type.to_str()));
                        }
                        if let Some(publisher_key) = publi.get_publisher_key() {
                            publisher_names.push(duckdb::types::Value::Text(publisher_key.key));
                        }
                    }
                    let mut venue_stmt = self.connection.prepare_cached(
                        format!("SELECT id, name, type \
                        FROM Venues \
                        WHERE (name, type) IN (\
                        SELECT unnest([{0}]), unnest([{1}]))",
                                venue_names.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", "),
                                venue_types.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", "),
                        ).as_str(),
                    ).expect("Failed while creating venue statement for publication");
                    let mut venue_ids = HashMap::new();
                    let mut items = Vec::new();
                    items.extend(venue_names);
                    items.extend(venue_types);
                    let mut query = venue_stmt.query(params_from_iter(items)).expect("Failed Query");
                    while let Some(row) =  query.next().unwrap() {
                        let key = row.get::<usize, String>(1).unwrap() + row.get::<usize, String>(2).unwrap().as_str();
                        let value: i32 = row.get(0).unwrap();
                        venue_ids.insert(key.clone(), value.clone());
                    }
                    drop(venue_stmt);
                    // Gather publisher ids
                    let mut publisher_ids = HashMap::new();
                    let mut publisher_stmt = self.connection.prepare_cached(
                        format!(
                            "SELECT id, name FROM Publishers WHERE name IN [{}]",
                            publisher_names.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", "),
                        ).as_str(),
                    ).expect("Failed while creating publisher statement for publication");
                    let mut query = publisher_stmt.query(params_from_iter(publisher_names)).expect("Failed Query");
                    while let Some(row) = query.next().unwrap() {
                        let key: String = row.get(1).unwrap();
                        let value: i32 = row.get(0).unwrap();
                        publisher_ids.insert(key.clone(), value.clone());
                    }
                    drop(publisher_stmt);
                    // Repackage data
                    let new_data: Vec<(&&Publication, Option<&i32>, Option<&i32>)> = data.iter().map(
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
                    let editor_names: Vec<duckdb::types::Value> = data.iter().map(
                        |x| {
                            duckdb::types::Value::Text(x.editor.clone().key)
                        }
                    ).collect();
                    let mut editor_ids = HashMap::new();
                    let mut editor_stmt = self.connection.prepare_cached(
                        format!(
                            "SELECT id, name FROM Editors WHERE name IN [{}]",
                            editor_names.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", ")
                        ).as_str()
                    ).expect("Failed while creating editor statement for publicationeditor");
                    let mut query = editor_stmt.query(params_from_iter(editor_names)).unwrap();
                    while let Some(row) = query.next().unwrap() {
                        let key: String =  row.get(1).unwrap();
                        let value: u32 = row.get(0).unwrap();
                        editor_ids.insert(key, value);
                    }
                    drop(editor_stmt);
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
                    let author_names: Vec<duckdb::types::Value> = data.iter().map(
                        |x| {
                            duckdb::types::Value::Text(x.author.clone().name)
                        }
                    ).collect();
                    let author_ids: Vec<duckdb::types::Value> = data.iter().map(
                        |x| {
                            duckdb::types::Value::Text(x.author.clone().id)
                        }
                    ).collect();
                    let mut author_keys = HashMap::new();
                    let mut author_stmt = self.connection.prepare_cached(
                        format!(
                            "SELECT key, id, name FROM Authors WHERE (name, id) IN (SELECT unnest([{0}]) , unnest([{1}]))",
                            author_names.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", "),
                            author_ids.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", "),
                        ).as_str()
                    ).expect("Failed while creating author statement for publicationAuthor");
                    let mut items = Vec::new();
                    items.extend(author_names);
                    items.extend(author_ids);
                    let mut query = author_stmt.query(params_from_iter(items)).unwrap();
                    while let Some(row) = query.next().unwrap() {
                        let key =  row.get::<usize, String>(1).unwrap() + row.get::<usize, String>(2).unwrap().as_str();
                        let value = row.get(0).unwrap();
                        author_keys.insert(key, value);
                    }
                    drop(author_stmt);
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
                    let author_names: Vec<duckdb::types::Value> = data.iter().map(
                        |x| {
                            duckdb::types::Value::Text(x.author.clone().name)
                        }
                    ).collect();
                    let author_ids: Vec<duckdb::types::Value> = data.iter().map(
                        |x| {
                            duckdb::types::Value::Text(x.author.clone().id)
                        }
                    ).collect();
                    let mut author_keys = HashMap::new();
                    let mut author_stmt = self.connection.prepare_cached(
                        format!(
                            "SELECT key, id, name FROM Authors WHERE (name, id) IN (SELECT unnest([{0}]) , unnest([{1}]))",
                            author_names.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", "),
                            author_ids.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", "),
                        ).as_str()
                    ).expect("Failed while creating author statement for publicationAuthor");
                    let mut items = Vec::new();
                    items.extend(author_names);
                    items.extend(author_ids);
                    let mut query = author_stmt.query(params_from_iter(items)).unwrap();
                    while let Some(row) = query.next().unwrap() {
                        let key =  row.get::<usize, String>(1).unwrap() + row.get::<usize, String>(2).unwrap().as_str();
                        let value = row.get(0).unwrap();
                        author_keys.insert(key, value);
                    }
                    drop(author_stmt);
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
                    let author_names: Vec<duckdb::types::Value> = data.iter().map(
                        |x| {
                            duckdb::types::Value::Text(x.author.clone().name)
                        }
                    ).collect();
                    let author_ids: Vec<duckdb::types::Value> = data.iter().map(
                        |x| {
                            duckdb::types::Value::Text(x.author.clone().id)
                        }
                    ).collect();
                    let mut author_keys = HashMap::new();
                    let mut author_stmt = self.connection.prepare_cached(
                        format!(
                            "SELECT key, id, name FROM Authors WHERE (name, id) IN (SELECT unnest([{0}]) , unnest([{1}]))",
                            author_names.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", "),
                            author_ids.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", "),
                        ).as_str()
                    ).expect("Failed while creating author statement for publicationAuthor");
                    let mut items = Vec::new();
                    items.extend(author_names);
                    items.extend(author_ids);
                    let mut query = author_stmt.query(params_from_iter(items)).unwrap();
                    while let Some(row) = query.next().unwrap() {
                        let key =  row.get::<usize, String>(1).unwrap() + row.get::<usize, String>(2).unwrap().as_str();
                        let value = row.get(0).unwrap();
                        author_keys.insert(key, value);
                    }
                    drop(author_stmt);
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
                    let author_names: Vec<duckdb::types::Value> = data.iter().map(
                        |x| {
                            duckdb::types::Value::Text(x.author.clone().name)
                        }
                    ).collect();
                    let author_ids: Vec<duckdb::types::Value> = data.iter().map(
                        |x| {
                            duckdb::types::Value::Text(x.author.clone().id)
                        }
                    ).collect();
                    let mut author_keys = HashMap::new();
                    let mut author_stmt = self.connection.prepare_cached(
                        format!(
                            "SELECT key, id, name FROM Authors WHERE (name, id) IN (SELECT unnest([{0}]) , unnest([{1}]))",
                            author_names.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", "),
                            author_ids.iter().map(|_| "?".to_string()).collect::<Vec<String>>().join(", "),
                        ).as_str()
                    ).expect("Failed while creating author statement for publicationAuthor");
                    let mut items = Vec::new();
                    items.extend(author_names);
                    items.extend(author_ids);
                    let mut query = author_stmt.query(params_from_iter(items)).unwrap();
                    while let Some(row) = query.next().unwrap() {
                        let key =  row.get::<usize, String>(1).unwrap() + row.get::<usize, String>(2).unwrap().as_str();
                        let value = row.get(0).unwrap();
                        author_keys.insert(key, value);
                    }
                    drop(author_stmt);
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
        F: Fn(&T) -> Vec<&(dyn ToSql)>
    {
        if items.is_empty() {
            return
        }

        let column_list = columns.join(", ");
        let num_columns = columns.len();
        let num_items = items.len();

        // Generate placeholders ($1, $2), ($3, $4), ...
        let placeholders: Vec<String> = (0..num_items)
            .map(|_| {
                let placeholders: Vec<String> = (0..num_columns)
                    .map(|_| "?".to_string())
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
        let params: Vec<&(dyn ToSql)> = items.iter()
            .flat_map(|item| extractor(item))
            .collect();

        let _ = self.connection.execute(query.as_str(), params_from_iter(params));
    }

    pub fn run_test_query(&self, query: &str, rows: usize, columns: usize) -> Result<u128, Box<dyn Error>> {
        let mut stmt = self.connection.prepare(query)?;
        let now = Instant::now();
        let _ = stmt.query(params![]).unwrap();
        let duration = now.elapsed().as_millis();
        if stmt.row_count() == rows && stmt.column_count() == columns {
            return Ok(duration)
        }
        Err("Result doesn't match expected size")?
    }
    
    pub fn close(self) -> Result<(), Box<dyn Error>> {
        // TODO Clear Data
        self.connection.close().expect("connection close failed");
        Ok(())
    }
}