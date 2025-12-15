use std::collections::HashMap;
use reqwest::header;
use serde::Deserialize;
use std::fs;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;
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
        fs::create_dir(format!("/data/{}", qlever_file.data.get("NAME").unwrap()).as_str())?;
        QLeverConnection::get_data(&qlever_file);
        println!("Finished Fetching Data");
        QLeverConnection::index(&qlever_file);
        println!("Finished Indexing");
        let conn = QLeverConnection::start(&qlever_file);
        println!("Finished startup");
        Ok(conn)
    }
    
    fn setup_config(dataset: &String) -> QleverFile {
        let dataset_parts: Vec<&str> = dataset.split(" ").collect();
        let target = match dataset_parts[0] {
            "dblp" => "https://raw.githubusercontent.com/ad-freiburg/qlever-control/refs/heads/main/src/qlever/Qleverfiles/Qleverfile.dblp",
            "osm-country" => "https://raw.githubusercontent.com/ad-freiburg/qlever-control/refs/heads/main/src/qlever/Qleverfiles/Qleverfile.osm-country",
            _ => panic!("Invalid dataset"),
        };
        let response = reqwest::blocking::get(target).unwrap();
        let mut content = response.text().unwrap();
        content = Self::extra_args(dataset_parts, &content);
        let sanitizied = &Self::sanitize_toml(content);
        return toml::from_str::<QleverFile>(sanitizied).unwrap();
    }

    fn extra_args(dataset_parts: Vec<&str>, content: &String) -> String {
        if dataset_parts.len() == 1 {
            return content.to_string()
        }
        match dataset_parts[0] {
            "osm-country" => {
                let continent = dataset_parts[1];
                let continent_str = format!("CONTINENT = {continent}");
                let country = dataset_parts[2];
                let country_str = format!("COUNTRY = {country}");
                let continent_regex = Regex::new(r"CONTINENT\s*=\s(europe)").unwrap();
                let country_regex = Regex::new(r"COUNTRY\s*=\s(switzerland)").unwrap();
                let mut new_content = continent_regex.replace(content.as_str(), continent_str).to_string();
                new_content = country_regex.replace(new_content.as_str(), country_str).to_string();
                return new_content
            }
            _ => unimplemented!()
        }
    }

    fn sanitize_toml(string: String) -> String {
        let comments = Regex::new(r"#\s.*\n?").unwrap();
        let jsons = Regex::new(r"=\s(.*)").unwrap();
        let mut new_toml = comments.replace_all(string.as_str(), "").to_string();
        new_toml = jsons.replace_all(new_toml.as_str(), |caps: &Captures| {
            let word = caps[1].to_string();
            format!("= \'{word}\'")
        }).to_string();
        new_toml.trim().to_string()
    }
    
    fn get_data(qlever_file: &QleverFile) {
        command_assist("bash",
                       &["-c", qlever_file.data.get("GET_DATA_CMD").unwrap().as_str()],
                       format!("/data/{}", qlever_file.data.get("NAME").unwrap()).as_str()
        ).unwrap()
    }
    
    fn index(qlever_file: &QleverFile) {
        // create settings json
        let name = qlever_file.data.get("NAME").unwrap().as_str();
        let path = format!("/data/{name}/{name}.settings.json");
        let mut file = File::create(path.clone()).unwrap();
        file.write_all(qlever_file.index.get("SETTINGS_JSON").unwrap().as_str().as_bytes()).unwrap();
        // Create Index
        let mut command = format!{
            "docker run --rm -u $(id -u):$(id -g) \
            -v /etc/localtime:/etc/localtime:ro \
            -v /data/{name}:/index \
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
            let multi_json = qlever_file.index.get("MULTI_INPUT_JSON").unwrap();
            let json: Value = serde_json::from_str(&multi_json).unwrap();
            let glob_cmd = format!("/data/{name}/{0}", json["for-each"].as_str().unwrap());
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

        command += format!(" | tee /index/{name}.index-log.txt'").as_str();
        command_assist("bash", &["-c", command.as_str()], ".").unwrap()
    }
    
    fn start(qlever_file: &QleverFile) -> QLeverConnection {
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
        let port = qlever_file.server.get("PORT").unwrap().as_str();

        let mut command = format!(
            "docker run -d --restart=unless-stopped \
            -u $(id -u):$(id -g) \
            -v /etc/localtime:/etc/localtime:ro \
            -v /data/{name}:/index \
            -p {port}:{port} \
            -w /index \
            --name qlever.server.{name} \
            --init \
            --entrypoint bash \
            docker.io/adfreiburg/qlever:latest -c 'ServerMain -i {name} -j 8 -p {port}"
        );
        if let Some(q_mem) = qlever_file.server.get("MEMORY_FOR_QUERIES") {
            command += format!(" -m {q_mem}").as_str();
        } else {
            command += " -m 5G";
        }
        if let Some(cache_max) = qlever_file.server.get("CACHE_MAX_SIZE") {
            command += format!(" -c {cache_max}").as_str();
        } else {
            command += " -c 5G";
        }
        if let Some(entry_max) = qlever_file.server.get("CACHE_MAX_SIZE_SINGLE_ENTRY") {
            command += format!(" -e {entry_max}").as_str();
        } else {
            command += " -e 1G";
        }
        if let Some(entry_count) = qlever_file.server.get("CACHE_MAX_NUM_ENTRIES") {
            command += format!(" -k {entry_count}").as_str();
        } else {
            command += " -k 200";
        }
        if let Some(timeout) = qlever_file.server.get("TIMEOUT") {
            command += format!(" -s {timeout}").as_str();
        }
        command += format!(" > /index/{name}.server-log.txt 2>&1'").as_str();
        command_assist("bash", &["-c", command.as_str()], ".").unwrap();
        let mut conn = QLeverConnection {
            qlever_file: qlever_file.clone(),
            docker_id: format!("qlever.server.{name}")
        };
        // Test connection
        let test_request = "SELECT * WHERE {?s ?p ?o} LIMIT 1";
        let mut times = 0;
        while times < 12 {
            let result = conn.do_query_request(test_request);
            match result {
                Ok(_) => {
                    break;
                }
                Err(e) => {
                    times += 1;
                    print!("{}", e);
                    sleep(Duration::from_secs(2))
                }
            }
        }

        conn
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

    pub fn run_test_query(&mut self, query: &str) -> u128 {
        let result: (u128, usize, usize) = self.do_query_request(query).expect("query failed");
        let name = self.qlever_file.data.get("NAME").unwrap().as_str();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(format!("/data/qlever.{}.log", name))
            .unwrap();
        let _ = file.write(
            format!("Query: {0}\nDuration: {1}\nResult Size: Columns {2} Rows {3}", query, result.0, result.2, result.1).as_bytes()
        );
        result.0
    }
    
    fn do_query_request(&mut self, query: &str) -> Result<(u128, usize, usize), Box<dyn Error>> {
        let port = self.qlever_file.server.get("PORT").unwrap().as_str();
        let mut headers = header::HeaderMap::new();
        headers.insert("Accept", "application/qlever-results+json".parse().unwrap());
        headers.insert("Content-type", "application/sparql-query".parse().unwrap());

        let client = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .unwrap();
        let query = query.to_string();
        let res = client.post(format!("http://127.0.0.1:{port}/"))
            .headers(headers)
            .body(query)
            .send();
        
        if let Err(e) = res {
            return Err(e.into());
        }
        
        let result: JsonResult = res.unwrap().json::<JsonResult>().expect("deserialize query result failed");
        
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

fn command_assist(command_str: &str, args: &[&str], current_dir: &str) -> Result<(), Box<dyn Error>> {
    let command = Command::new(command_str)
        .args(args)
        .current_dir(current_dir)
        .output()
        .expect(("Failed executing command ".to_string() + command_str + " " + args.join(" ").as_str()).as_ref());
    println!("status: {}", &command.status);
    println!("stdout:\n{}", String::from_utf8_lossy(&command.stdout));
    println!("stderr:\n{}", String::from_utf8_lossy(&command.stderr));
    if command.status.success() {
        Ok(())
    } else {
        Err(format!("Command failed status code: {0}\nCommand: {1} {2:?}", command.status, command_str, args).into())
    }
}