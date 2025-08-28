use reqwest::Client;
use serde::Deserialize;
use std::env;
use std::error::Error;
use std::process::Command;
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
            return Err(format!(
                "Result doesn't match expected size:\n Expected: Rows {0}, Columns {1}\n Got: Rows {2} Columns {3}",
                rows,
                columns,
                result.1,
                result.2
            ).into())
        }
        Ok(result.0)
    }
    
    pub fn new(dataset: &String) -> Result<QLeverConnection, Box<dyn Error>> {
        let qlever_file = match dataset.as_str() {
            "dblp" => "dblp".to_string(),
            _ => return Err("Invalid dataset for qlever".into()),
        };
        let mut conn = QLeverConnection { host: "http://localhost:7027".into() , qlever_file };
        conn.startup()?;
        Ok(conn)
    }
    
    fn startup(&mut self) -> Result<(), Box<dyn Error>> {
        command_assist("mkdir", &[format!("./src/data/{}", &self.qlever_file).as_str()], "/usr/src/bachelor_thesis")?;
        command_assist_qlever(&["setup-config", format!("{}", &self.qlever_file).as_str()], &self.qlever_file)?;
        command_assist_qlever(&["get-data"], &self.qlever_file)?;
        command_assist_qlever(&["index"], &self.qlever_file)?;
        command_assist_qlever(&["start"], &self.qlever_file)?;
        Ok(())
    }
     
    pub fn stop(self) -> Result<(), Box<dyn Error>> {
        command_assist_qlever(&["stop"], &self.qlever_file)?;
        command_assist("rm", &["-r", format!("./src/data/{}", &self.qlever_file).as_str()], "/usr/src/bachelor_thesis")?;
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

fn command_assist(command: &str, args: &[&str], current_dir: &str) -> Result<(), Box<dyn Error>> {
    let command = Command::new(command)
        .args(args)
        .current_dir(current_dir)
        .output()
        .expect(("Failed executing command ".to_string() + command + " " + args.join(" ").as_str()).as_ref());
    println!("status: {}", &command.status);
    println!("stdout:\n{}", String::from_utf8_lossy(&command.stdout));
    println!("stderr:\n{}", String::from_utf8_lossy(&command.stderr));
    if command.status.success() {
        Ok(())
    } else {
        Err(format!("Command failed status code: {}", command.status).into())
    }
}

fn command_assist_qlever(args: &[&str], qlever_file: &str) -> Result<(), Box<dyn Error>> {
    let command = Command::new("qlever")
        .args(args)
        .env("PATH", format!("/usr/qlever-venv/bin:{}", env::var("PATH").unwrap()))
        .env("VIRTUAL_ENV", "/usr/qlever-venv")
        .current_dir(format!("/usr/src/bachelor_thesis/src/data/{}", &qlever_file).as_str())
        .output()
        .expect(("Failed to execute command ".to_string() + args.join(" ").as_str()).as_str());

    println!("status: {}", &command.status);
    println!("stdout:\n{}", String::from_utf8_lossy(&command.stdout));
    println!("stderr:\n{}", String::from_utf8_lossy(&command.stderr));
    if command.status.success() {
        Ok(())
    } else {
        Err(format!("Command failed status code: {}", command.status).into())
    }
}