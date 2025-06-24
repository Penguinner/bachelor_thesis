use postgres::types::ToSql;
use crate::parser::Parser;
use postgres::{Client, NoTls, Row};
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::time::Instant;
use bollard::Docker;
use bollard::models::ContainerCreateBody;
use bollard::query_parameters::CreateContainerOptionsBuilder;
use futures::TryStreamExt;
use tokio::runtime::Runtime;

pub struct PostgresConnection {
    client: Client,
    dataset: String,
    docker_id: String,
}

impl PostgresConnection {

    pub fn new(dataset: &String) -> Result<Self, Box<dyn Error>> {
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
            
            let config = ContainerCreateBody {
                image: Some("postgres:latest".into()),
                
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
        let client = Client::connect("host=http://localhost:5432 user=postgres", NoTls).unwrap();
        let mut conn = PostgresConnection { client, dataset: dataset.into(), docker_id: id};
        // TODO add more datasets
        match dataset.as_str() {
            "dblp" => {
                conn.create_tables_dblp();
                conn.insert_dblp_data("./data/dblp.xml".into());
            },
            _ => { return Err("dataset could not be resolved for postgres Connection".into())}
        }

        Ok(conn)
    }

    pub fn create_tables_dblp(&mut self) {
        let mut file = File::open("create_tables_dblp.sql").unwrap();
        let mut query = String::new();
        file.read_to_string(&mut query).unwrap();
        self.client.batch_execute(&query).unwrap();
    }

    pub fn insert_dblp_data(&mut self, file: String) {
        let parser = Parser::new(file.as_ref());
        let mut ref_ops = Vec::new();
        for record in parser {
            let ops: (Vec<(String, Vec<String>)>, Vec<(String, Vec<String>)>) = record.generate_sql_ops('$');
            ref_ops.append(&mut ops.1.clone());
            for op in ops.0 {
                let params: Vec<&(dyn ToSql + Sync)> =
                    op.1.iter().map(|s| s as &(dyn ToSql + Sync)).collect();
                self.client.execute(&op.0, &params).unwrap();
            }
        }
        
        for op in ref_ops {
            let params: Vec<&(dyn ToSql + Sync)> =
                op.1.iter().map(|s| s as &(dyn ToSql + Sync)).collect();
            self.client.execute(&op.0, &params).unwrap();
        }
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
    
    pub fn close(mut self) -> Result<(), Box<dyn Error>> {
        // TODO Clear Data
        self.client.close().expect("close failed");
        //Stop docker container
        let rt = Runtime::new()?;
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