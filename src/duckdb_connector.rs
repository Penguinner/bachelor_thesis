use crate::{AFFILIATIONS_FILE, ALIAS_FILE, AUTHOR_FILE, AUTHOR_WEBSITES_FILE, EDITOR_FILE, PUBLICATION_AUTHORS_FILE, PUBLICATION_EDITOR_FILE, PUBLICATION_FILE, PUBLISHER_FILE, REFERENCE_FILE, RESOURCES_FILE, VENUE_FILE};
use duckdb::{params, Connection};
use std::error::Error;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::time::Instant;

pub struct DuckDBConnection {
    connection: Connection,
    dataset: String,
}

impl DuckDBConnection {
    pub fn new(dataset: &String) -> Result<DuckDBConnection,  Box<dyn Error >> {
        let dataset_parts: Vec<&str> = dataset.split(" ").collect();
        let mut conn = DuckDBConnection { connection: Connection::open("db.duckdb").unwrap(), dataset: dataset .to_string() };
        // TODO Add more datasets
        match dataset_parts[0] {
            "dblp" => {
                conn.create_tables_dblp();
                conn.insert_dblp_data();
            },
            "osm-country" => {
                conn.load_spatial_module();
                conn.load_osm_country_data();
            }
            _ => { return Err("dataset could not be resolved for duckdb Connection".into())}
        }

        Ok(conn)
    }

    fn load_spatial_module(&mut self) {
        self.connection.execute("INSTALL spatial;", []).unwrap();
        self.connection.execute("LOAD spatial;", []).unwrap();
    }

    fn load_osm_country_data(&mut self) {
        let dataset_parts: Vec<&str> = self.dataset.split(" ").collect();
        let continent = dataset_parts[1];
        let country = dataset_parts[2];
        let url = format!("https://download.geofabrik.de/{continent}/{country}-latest.osm.pbf");
        let response = reqwest::blocking::get(url).unwrap();
        let file_path = format!("/data/{country}-latest.osm.pbf");
        let file = File::create(&file_path).unwrap();
        file.write_all(response.bytes().unwrap()).unwrap();
        let query = format!("CREATE TABLE osm AS SELECT * FROM ST_ReadOSM({file_path})");
        self.connection.execute(&query, []).unwrap();
    }

    pub fn create_tables_dblp(&mut self) {
        let mut file = File::open("create_tables_dblp.sql").unwrap();
        let mut query = String::new();
        file.read_to_string(&mut query).unwrap();
        query = format!("BEGIN;\n {}\n COMMIT;", query);
        self.connection.execute_batch(&query).unwrap();
        println!("Created Tables DBLP");
    }

    pub fn insert_dblp_data(&mut self) {
        let query = format!(
            "BEGIN;\n\
             COPY Venues FROM '{0}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
             COPY Publishers FROM '{1}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
             COPY Editors FROM '{2}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
             COPY Authors FROM '{3}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
             COPY Publications FROM '{4}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
             COPY Resources FROM '{5}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
             COPY PublicationEditors FROM '{6}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
             COPY Reference FROM '{7}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
             COPY PublicationAuthors FROM '{8}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
             COPY AuthorWebsites FROM '{9}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
             COPY Affiliations FROM '{10}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
             COPY Alias FROM '{11}' (FORMAT CSV, DELIMITER E'\\t', HEADER true);\n\
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
        println!("Inserted DBLP data into DuckDB");
    }

    pub fn run_test_query(&self, query: &str) -> u128 {
        let mut stmt = self.connection.prepare(query).unwrap();
        let now = Instant::now();
        let _ = stmt.query(params![]).unwrap();
        let duration = now.elapsed().as_millis();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(format!("/data/duckdb.{}.log", self.dataset))
            .unwrap();
        let _ = file.write(
            format!("Query: {0}\nDuration: {1}\nResult Size: Columns {2} Rows {3}", query, duration, stmt.column_count(), stmt.row_count()).as_bytes()
        );
        duration
    }
    
    pub fn close(self) -> Result<(), Box<dyn Error>> {
        fs::remove_file("db.duckdb").unwrap();
        self.connection.close().expect("connection close failed");
        Ok(())
    }
}