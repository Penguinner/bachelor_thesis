use std::fs::File;
use std::io::ErrorKind;
use postgres::{Client, NoTls};

pub struct PostgresConnection {
    client: Client,
}

impl PostgresConnection {

    pub fn new(host: String, user: String) -> Self {
        let client = Client::connect(format!("host={0} user={1}", host, user), NoTls).unwrap();
        PostgresConnection {
            client
        }
    }

    pub fn create_tables_dblp(&mut self) {
        let mut file = File::open("create_tables.sql").unwrap();
        let mut query = String::new();
        file.read_to_string(&mut query).unwrap();
        self.client.batch_execute(&query).unwrap();
    }

    pub fn insert_dblp_data(&mut self, file: String) {
        let parser = Parser::new(file.as_ref());
        for record in parser {
            let ops = record.generate_sql_ops();
            for op in ops {
                self.client.execute(op.as_ref(),params![]).unwrap();
            }
        }
    }

    pub fn run_test_query(&self, query: &str, params: Vec<str>, rows: usize, colums: usize) -> Result<u128>{
        let now = Instant::now();
        let result : Vec<Row> = self.client.query(query, params)?;
        let duration = now.elapsed().as_millis();
        if result.len() == rows && result.get(0).len() == columns {
            Ok(duration)
        }
        Err(Error::new(ErrorKind::Other, "Result doesn't match expected size"))
    }
}