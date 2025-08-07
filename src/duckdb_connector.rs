use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::time::Instant;
use duckdb::{appender_params_from_iter, params, params_from_iter, AppenderParams, Connection, ToSql};
use duckdb::types::Value::Array;
use crate::parser::Parser;
use crate::{AFFILIATIONS_FILE, ALIAS_FILE, AUTHOR_FILE, AUTHOR_WEBSITES_FILE, EDITOR_FILE, PUBLICATION_AUTHORS_FILE, PUBLICATION_EDITOR_FILE, PUBLICATION_FILE, PUBLISHER_FILE, REFERENCE_FILE, RESOURCES_FILE, VENUE_FILE};

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
                conn.insert_dblp_data();
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

    pub fn insert_dblp_data(&mut self) {
        let query = format!(
            "BEGIN;\n\
             COPY Venues FROM '{0}';\n\
             COPY Publishers FROM '{1}';\n\
             COPY Editors FROM '{2}';\n\
             COPY Authors FROM '{3}';\n\
             COPY Publications FROM '{4}';\n\
             COPY Resources FROM '{5}';\n\
             COPY PublicationEditors FROM '{6}';\n\
             COPY Reference FROM '{7}';\n\
             COPY PublicationAuthors FROM '{8}';\n\
             COPY AuthorWebsites FROM '{9}';\n\
             COPY Affiliations FROM '{10}';\n\
             COPY Alias FROM '{11}';\n\
             END;",
            VENUE_FILE,
            PUBLISHER_FILE,
            EDITOR_FILE,
            AUTHOR_FILE,
            PUBLICATION_FILE,
            RESOURCES_FILE,
            PUBLICATION_EDITOR_FILE,
            REFERENCE_FILE,
            PUBLICATION_AUTHORS_FILE,
            AUTHOR_WEBSITES_FILE,
            AFFILIATIONS_FILE,
            ALIAS_FILE
        );
        self.connection.execute_batch(&query).unwrap();
        println!("Inserted DBLP data");
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