use std::collections::HashMap;
use reqwest::Client;
use serde::Deserialize;
use std::fs;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::process::Command;
use bollard::Docker;
use glob::glob;
use regex::{Captures, Regex};
use serde_json::Value;
use tokio::runtime::Runtime;

pub struct QLeverConnection {
    docker_id: String,
    qlever_file: QleverFile
}

impl QLeverConnection {
    
    pub fn new(dataset: &String) -> Result<QLeverConnection, Box<dyn Error>> {
        let mut qlever_file = QLeverConnection::setup_config(dataset);
        println!("Finished Setup Config");
        qlever_file.replace_internal_variables();
        // Create directory
        fs::create_dir(qlever_file.data.get("NAME").unwrap().as_str())?;
        QLeverConnection::get_data(&qlever_file);
        println!("Finished Fetching Data");
        QLeverConnection::index(&qlever_file);
        println!("Finished Indexing");
        let conn = QLeverConnection::start(&qlever_file);
        println!("Finished startup");
        Ok(conn)
    }
    
    fn setup_config(dataset: &String) -> QleverFile {

        let target = match dataset.as_str() {
            "dblp" => Some("https://raw.githubusercontent.com/ad-freiburg/qlever-control/refs/heads/main/src/qlever/Qleverfiles/Qleverfile.dblp"),
            "osm-country" => Some("https://raw.githubusercontent.com/ad-freiburg/qlever-control/refs/heads/main/src/qlever/Qleverfiles/Qleverfile.osm-country"),
            _ => None,
        };
        
        if let Some(target) = target {
            let response = reqwest::blocking::get(target).unwrap();
            let content = response.text().unwrap();
            return toml::from_str::<QleverFile>(&Self::sanitize_toml(content)).unwrap();
        }
        panic!("Missing config")
    }

    fn sanitize_toml(string: String) -> String {
        let comments = Regex::new(r"#.*\n").unwrap();
        let jsons = Regex::new(r"=\s(.*)").unwrap();
        let mut new_toml = comments.replace_all(string.as_str(), "").to_string();
        new_toml = jsons.replace_all(new_toml.as_str(), |caps: &Captures| {
            let word = caps[1].to_string();
            format!("= \'{word}\'")
        }).to_string();
        new_toml.trim().to_string()
    }
    
    fn get_data(qlever_file: &QleverFile) {
        command_assist("bash", &["-c", qlever_file.data.get("GET_DATA_CMD").unwrap().as_str()], qlever_file.data.get("NAME").unwrap().as_str()).unwrap()
    }
    
    fn index(qlever_file: &QleverFile) {
        // create settings json
        let name = qlever_file.data.get("NAME").unwrap().as_str();
        let path = format!("{name}/{name}.settings.json");
        let mut file = File::create(path.clone()).unwrap();
        file.write_all(qlever_file.index.get("SETTINGS_JSON").unwrap().as_str().as_bytes()).unwrap();
        // Create Index
        let mut command = format!{
            "pwd; ls -la; docker run --rm -u $(id -u):$(id -g) \
            -v /etc/localtime:/etc/localtime:ro \
            -v {name}:/index \
            -w /index \
            --name qlever.index.{name} \
            --init \
            --entrypoint bash \
            docker.io/adfreiburg/qlever:latest \
            -c '"
        };
        // Add files arguments
        if qlever_file.index.contains_key("MULTI_INPUT_JSON") {
            command += format!("IndexBuilderMain \
            -i {name} \
            -s {name}.settings.json \
            --vocabulary-type on-disk-compressed").as_str();
            let mulit_json = qlever_file.index.get("MULTI_INPUT_JSON").unwrap();
            let json: Value = serde_json::from_str(&mulit_json).unwrap();
            let glob_cmd = format!("{name}/{0}", json["for-each"].as_str().unwrap());
            println!("{:?}", glob_cmd);
            for file in glob(glob_cmd.as_str()).unwrap() {
                let file_path = file.unwrap();
                let file_name = file_path.file_name().unwrap().to_str().unwrap();
                let cmd = json["cmd"].as_str().unwrap().replace("{}", file_name);
                command += format!(" -f <({cmd}) -g - -F ttl -p false").as_str();
            }
        } else {
            command += qlever_file.index.get("CAT_INPUT_FILES").unwrap();
            let stxxl = qlever_file.index.get("STXXL_MEMORY").unwrap();
            command += format!(
                " IndexBuilderMain \
                -i {name} \
                -s {name}.settings.json \
                --vocabulary-type on-disk-compressed -F ttl -f - \
                --stxxl-memory {stxxl}\
                "
            ).as_str();
        }

        command += format!(" | tee {name}.index-log.txt'").as_str();
        println!("{}", command);
        command_assist("bash", &["-c", command.as_str()], name).unwrap()
    }
    
    fn start(qlever_file: &QleverFile) -> QLeverConnection {
        panic!("Reached Start");
        // docker run -d --restart=unless-stopped
        // -u $(id -u):$(id -g)
        // -v /etc/localtime:/etc/localtime:ro
        // -v $(pwd):/index
        // -p 7015:7015
        // -w /index
        // --name qlever.server.dblp
        // --init
        // --entrypoint bash
        // docker.io/adfreiburg/qlever:latest -c
        // 'ServerMain -i dblp -j 8 -p 7015 -m 10G -c 5G -e 1G -k 200 -s 300s -a dblp_yGJxTdx6CXRb > dblp.server-log.txt 2>&1'
        let name = qlever_file.data.get("NAME").unwrap().as_str();
        let uid = String::from_utf8(
            Command::new("id")
                .arg("-u")
                .output()
                .unwrap()
                .stdout
        ).unwrap();
        let gid = String::from_utf8(
            Command::new("id")
                .arg("-g")
                .output()
                .unwrap()
                .stdout
        ).unwrap();
        let port = qlever_file.server.get("PORT").unwrap().as_str();

        let mut command = format!(
            "docker run -d --restart=unless-stopped \
            -u {uid}:{gid} \
            -v /etc/localtime:/etc/localtime:ro \
            -v /index:/index \
            -p {port}:{port} \
            -w /index \
            --name qlever.server.{name} \
            --init \
            --entrypoint bash \
            docker.io/adfreiburg/qlever:latest -c 'ServerMain -i {name} -j 8 -p {port} "
        );
        if let Some(q_mem) = qlever_file.server.get("MEMORY_FOR_QUERIES") {
            command += format!("-m {q_mem}").as_str();
        } else {
            command += "-m 5G";
        }
        if let Some(cache_max) = qlever_file.server.get("CACHE_MAX_SIZE") {
            command += format!("-c {cache_max}").as_str();
        } else {
            command += "-c 5G";
        }
        if let Some(entry_max) = qlever_file.server.get("CACHE_MAX_SIZE_SINGLE_ENTRY") {
            command += format!("-e {entry_max}").as_str();
        } else {
            command += "-e 1G";
        }
        if let Some(entry_count) = qlever_file.server.get("CACHE_MAX_NUM_ENTRIES") {
            command += format!("-k {entry_count}").as_str();
        } else {
            command += "-k 200";
        }
        if let Some(timeout) = qlever_file.server.get("TIMEOUT") {
            command += format!("-s {timeout}").as_str();
        }
        command += format!("> {name}.server-log.txt 2>&1'").as_str();
        command_assist("bash", &["-c", command.as_str()], name).unwrap();
        QLeverConnection {
            qlever_file: qlever_file.clone(),
            docker_id: format!("qlever.server.{name}")
        }
    }
    
    pub fn stop(&self) -> Result<(), Box<dyn Error>> {
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
    
    async fn do_query_request(&mut self, query: &str) -> Result<(u128, usize, usize), Box<dyn Error>> {
        let client = Client::new();
        let response = client.post("http://localhost:".to_string() + self.qlever_file.server.get("PORT").unwrap().as_str())
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

#[derive(Deserialize, Clone)]
pub struct QleverFile {
    pub data: HashMap<String, String>,
    pub index: HashMap<String, String>,
    pub server: HashMap<String, String>,
    #[serde(flatten)]
    pub others: HashMap<String, toml::Value>,
}

impl QleverFile {
    pub fn replace_internal_variables(&mut self) {
        let regex = Regex::new(r"\$\{(?<prefix>\w+:)?(?<key>\w+)}").unwrap();
        // Iterate over Data
        self.data = self.data.iter().map(|(orig_key, value)| {
            let mut changed = value.clone();
            while regex.is_match(changed.as_str()) {
                changed = regex.replace_all(changed.as_str(), |cap: &Captures|{
                    let prefix = cap.name("prefix");
                    let key = cap.name("key").unwrap().as_str();
                    match prefix {
                        Some(prefix) if prefix.as_str() == "data:" => self.data.get(key).unwrap(),
                        Some(prefix) if prefix.as_str() == "index:" => self.index.get(key).unwrap(),
                        Some(prefix) if prefix.as_str() == "server:" => self.server.get(key).unwrap(),
                        None | Some(_) => self.data.get(key).unwrap()
                    }
                }
                ).to_string();
            }
            (orig_key.to_string(), changed)
        }).collect();
        // Iterate over Index
        self.index = self.index.iter().map(|(orig_key, value)| {
            let mut changed = value.clone();
            while regex.is_match(changed.as_str()) {
                changed = regex.replace_all(changed.as_str(), |cap: &Captures|{
                    let prefix = cap.name("prefix");
                    let key = cap.name("key").unwrap().as_str();
                    match prefix {
                        Some(prefix) if prefix.as_str() == "data:" => self.data.get(key).unwrap(),
                        Some(prefix) if prefix.as_str() == "index:" => self.index.get(key).unwrap(),
                        Some(prefix) if prefix.as_str() == "server:" => self.server.get(key).unwrap(),
                        None | Some(_) => self.index.get(key).unwrap()
                    }
                }
                ).to_string();
            }
            (orig_key.to_string(), changed)
        }).collect();
        // Iterate over Server
        self.server = self.server.iter().map(|(orig_key, value)| {
            let mut changed = value.clone();
            while regex.is_match(changed.as_str()) {
                changed = regex.replace_all(changed.as_str(), |cap: &Captures|{
                    let prefix = cap.name("prefix");
                    let key = cap.name("key").unwrap().as_str();
                    match prefix {
                        Some(prefix) if prefix.as_str() == "data:" => self.data.get(key).unwrap(),
                        Some(prefix) if prefix.as_str() == "index:" => self.index.get(key).unwrap(),
                        Some(prefix) if prefix.as_str() == "server:" => self.server.get(key).unwrap(),
                        None | Some(_) => self.server.get(key).unwrap()
                    }
                }
                ).to_string();
            }
            (orig_key.to_string(), changed)
        }).collect();
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