use std::error::Error;
use std::process::Command;
use reqwest::Client;
use serde::Deserialize;
use tokio::runtime::Runtime;

pub struct QLeverConnection {
    host: String,
    qlever_file: String,
}

impl QLeverConnection {
    pub fn run_test_query(&mut self, query: &str, rows: usize, columns: usize) -> Result<u128, Box<dyn Error>> {
        let rt = Runtime::new()?;
        let handle = rt.handle();
        
        let result: (u128, usize, usize) = handle.block_on(self.do_query_request(query)).expect("query failed");
        if result.1 != rows || result.2 != columns {
            return Err("Result doesn't match expected size".into())
        }
        Ok(result.0)
    }
    
    pub fn new(host: String, qlever_file: String) -> QLeverConnection {
        let mut conn = QLeverConnection { host , qlever_file };
        conn.startup().expect("Failed while starting qlever connection");
        conn
    }
    
    fn startup(&mut self) -> Result<(), Box<dyn Error>> {
        Command::new(format!("qlever setup-config {}", self.qlever_file)).status().expect("qlever setup-config failed");
        Command::new("qlever get-data").status().expect("qlever get data failed");
        Command::new("qlever index").status().expect("qlever index failed");
        Command::new("qlever start").status().expect("qlever start failed");
        Ok(())
    }
    
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        Command::new("qlever stop").status().expect("qlever stop failed");
        Ok(())
    }
    
    async fn do_query_request(&mut self, query: &str) -> Result<(u128, usize, usize), Box<dyn Error>> {
        let client = Client::new();
        let response = client.post(self.host.clone())
            .header("Accept", "application/qlever_results+json")
            .header("Content-Type", "application/sparql-query")
            .body(query.to_string())
            .send()
            .await
            .expect("qlever query failed");
        
        let result: JsonResult = response.json::<JsonResult>().await.expect("deserialize query result failed");
        
        let time: u128 = result.time.total.chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse::<u128>()
            .expect("parse time failed");
        
        Ok((time, result.runtime.query_execution_tree.result_rows, result.runtime.query_execution_tree.result_cols))
    }
}

// Response deserialization struct
#[derive(Deserialize)]
struct JsonResult {
    #[serde(rename = "runtimeInformation")]
    runtime: JsonRuntimeInfo,
    time: JsonTime,
}

#[derive(Deserialize)]
struct JsonTime {
    #[serde(rename = "computeResult")]
    compute_result: String,
    total: String,
}

#[derive(Deserialize)]
struct JsonRuntimeInfo {
    query_execution_tree : JsonQueryExecTree
}

#[derive(Deserialize)]
struct JsonQueryExecTree {
    result_cols: usize,
    result_rows: usize,
}