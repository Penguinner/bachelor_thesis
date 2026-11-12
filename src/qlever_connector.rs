use bollard::Docker;
use bollard::query_parameters::{PruneBuildOptions, PruneImagesOptions, PruneVolumesOptions};
use core::panic;
use glob::glob;
use log::{error, info, trace};
use regex::{Captures, Regex};
use reqwest::header;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::fs::{File, remove_dir_all};
use std::io::Write;
use std::process::Command;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

use crate::Dataset;

/// Connection for QLever database
pub struct QLeverConnection {
    docker_id: String,
    qlever_file: QleverFile,
}

impl QLeverConnection {
    /// Create a new [`QLeverConnection`]
    pub fn new(dataset: &Dataset) -> Result<QLeverConnection, Box<dyn Error>> {
        // Read Config for dataset
        let mut qlever_file = QLeverConnection::setup_config(dataset);
        trace!("Finished Setup Config");
        qlever_file.replace_internal_variables();
        // Create directory for dataset
        fs::create_dir(
            format!(
                "/extern/data/{}-qlever",
                qlever_file.data.get("NAME").unwrap()
            )
            .as_str(),
        )?;
        // Get Data for dataset with qlever file command
        QLeverConnection::get_data(&qlever_file);
        trace!("Finished Fetching Data");
        // Start Indexing
        QLeverConnection::index(&qlever_file);
        trace!("Finished Indexing");
        let conn = QLeverConnection::start(&qlever_file);
        trace!("Finished startup");
        Ok(conn)
    }

    /// Cooldown by using the clear-cache-complete command for qlever
    pub fn cooldown(&mut self) {
        let port = self.qlever_file.server.get("PORT").unwrap().as_str();
        let client = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .unwrap();
        let res = client
            .get(format!(
                "http://127.0.0.1:{port}/?cmd=clear-cache-complete&access-token=benchmark"
            ))
            .send();
        if let Err(e) = res {
            panic!("{}", e)
        }
    }

    /// Get the QLeverfile from the qlever-control repository and parse it for our purpose
    fn setup_config(dataset: &Dataset) -> QleverFile {
        // Fetch File
        let target = match dataset {
            Dataset::Dblp => {
                "https://raw.githubusercontent.com/ad-freiburg/qlever-control/refs/heads/main/src/qlever/Qleverfiles/Qleverfile.dblp"
            }
            Dataset::OsmPlanet => {
                "https://raw.githubusercontent.com/qlever-dev/qlever-control/refs/heads/main/src/qlever/Qleverfiles/Qleverfile.osm-planet"
            }
        };
        let response = reqwest::blocking::get(target).unwrap();
        let content = response.text().unwrap();
        // Convert to QLeverfile struct
        let sanitized = sanitize_toml(content);
        toml::from_str::<QleverFile>(sanitized.as_str()).unwrap()
    }

    /// Get Data for dataset defined by GET_DATA_CMD in QLeverfile
    fn get_data(qlever_file: &QleverFile) {
        command_assist(
            "bash",
            &["-c", qlever_file.data.get("GET_DATA_CMD").unwrap().as_str()],
            format!(
                "/extern/data/{}-qlever",
                qlever_file.data.get("NAME").unwrap()
            )
            .as_str(),
        )
        .unwrap();
    }

    /// Start indexing for QLever
    fn index(qlever_file: &QleverFile) {
        // create settings json
        let name = qlever_file.data.get("NAME").unwrap().as_str();
        let path = format!("/extern/data/{name}-qlever/{name}.settings.json");
        let mut file = File::create(path.clone()).unwrap();
        file.write_all(
            qlever_file
                .index
                .get("SETTINGS_JSON")
                .unwrap()
                .as_str()
                .replace("\'", "\"")
                .as_bytes(),
        )
        .unwrap();
        // Create Index
        let mut command = format! {
            "docker run --rm -u $(id -u):$(id -g) \
            -v /etc/localtime:/etc/localtime:ro \
            -v ~/extern/data/{name}:/index \
            -w /index \
            --name qlever.index.{name} \
            --init \
            --entrypoint bash \
            docker.io/adfreiburg/qlever:latest \
            -c '"
        };

        // Add files arguments
        let mut vocab = "on-disk-compressed";
        if let Some(vocabulary) = qlever_file.index.get("VOCABULARY_TYPE") {
            vocab = vocabulary;
        }
        if qlever_file.index.contains_key("MULTI_INPUT_JSON") {
            // MULTI_INPUT_JSON Format
            command += format!(
                "qlever-index \
            -i {name} \
            -s {name}.settings.json \
            --vocabulary-type {vocab}"
            )
            .as_str();
            let multi_json = qlever_file.index.get("MULTI_INPUT_JSON").unwrap().as_str();
            let json: Value =
                serde_json::from_str(multi_json.replace("\'", "\"").as_str()).unwrap();
            let cmd = json["cmd"].as_str().unwrap();
            if cmd.contains("gunzip") {
                let glob_cmd = format!(
                    "/extern/data/{name}/{0}",
                    json["for-each"].as_str().unwrap()
                );
                for file in glob(glob_cmd.as_str()).unwrap() {
                    let file_path = file.unwrap();
                    let file_name = file_path.file_name().unwrap().to_str().unwrap();
                    let gunzip_cmd = json["cmd"].as_str().unwrap().replace("{}", file_name);
                    command += format!(" -f <({gunzip_cmd}) -g - -F ttl -p false").as_str();
                }
            } else if cmd.contains("lbzcat") {
                command += format!(" -f {}", cmd).as_str();
            } else {
                panic!("Unknown cmd for getting qlever data {cmd}");
            }
        } else {
            // CAT_INPUT Format
            command += qlever_file
                .index
                .get("CAT_INPUT_FILES")
                .unwrap()
                .as_str()
                .replace("zcat", "ulimit -Sn 500000 && zcat")
                .as_str();
            let stxxl = qlever_file.index.get("STXXL_MEMORY").unwrap();

            command += format!(
                "| qlever-index \
                -i {name} \
                -s {name}.settings.json \
                --vocabulary-type {vocab} -F ttl -f - \
                --stxxl-memory {stxxl}\
                "
            )
            .as_str();
        }

        if let Some(value) = qlever_file.index.get("PARALLEL_PARSING")
            && value == "true"
        {
            command += " -p true";
        }

        command += format!(" | tee /index/{name}.index-log.txt'").as_str();
        info!("Running index command: {command}");
        // Execute Index command
        command_assist("bash", &["-c", command.as_str()], ".").unwrap()
    }

    /// Start the QLever server
    fn start(qlever_file: &QleverFile) -> QLeverConnection {
        // Get name and port
        let name = qlever_file.data.get("NAME").unwrap().as_str();
        let port = qlever_file.server.get("PORT").unwrap().as_str();

        // Parse the start command together from QLeverfile
        let mut command = format!(
            "docker run -d --restart=unless-stopped \
            -u $(id -u):$(id -g) \
            -v /etc/localtime:/etc/localtime:ro \
            -v ~/extern/data/{name}:/index \
            -p {port}:{port} \
            -w /index \
            --name qlever.server.{name} \
            --init \
            --entrypoint bash \
            docker.io/adfreiburg/qlever:latest -c 'qlever-server -i {name} -j 8 -p {port}"
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
        command += format!(" -s 600s -a benchmark > /index/{name}.server-log.txt 2>&1'").as_str();
        // Execute Start Command
        info!("Running start command: {command}");
        command_assist("bash", &["-c", command.as_str()], ".").unwrap();
        let mut conn = QLeverConnection {
            qlever_file: qlever_file.clone(),
            docker_id: format!("qlever.server.{name}"),
        };
        // Test connection
        conn.wait_for_connect();
        conn
    }

    /// Tests if the QLever server is ready to receive queries
    fn wait_for_connect(&mut self) -> () {
        let timeout = Duration::from_secs(600);
        let start = Instant::now();
        let mut attempts: u64 = 0;
        loop {
            attempts += 1;
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                panic!("Qlever at not ready after {timeout:?}")
            }

            // Do Test request
            let result = self.do_query_request("SELECT * WHERE {?s ?p ?o} LIMIT 1");
            match result {
                Ok(_) => {
                    return
                }
                Err(e) => {
                    error!("Failure during HTTP request: {:?}", e);
                }
            }

            // Increase Backoff time up to 3s
            let backoff_ms = std::cmp::min(200u64 << (attempts - 1).min(4), 3000);
            let sleep_for = Duration::from_millis(backoff_ms.min(remaining.as_millis() as u64));

            info!(
                "[ waiting for qlever ] attempt {attempts}, \
                elapsed={:?}, retrying in {:?}",
                start.elapsed(),
                sleep_for,
            );
            sleep(sleep_for);
        }
    }

    /// Stop the QLever docker container
    pub fn stop(&self) -> Result<(), Box<dyn Error>> {
        //Stop docker container
        trace!("Stopping qlever docker container");
        let rt = Runtime::new().unwrap();
        let handle = rt.handle();

        handle.block_on(async {
            let docker = Docker::connect_with_defaults().unwrap();

            docker
                .stop_container(
                    self.docker_id.as_str(),
                    None::<bollard::query_parameters::StopContainerOptions>,
                )
                .await
                .unwrap();

            docker
                .remove_container(
                    self.docker_id.as_str(),
                    None::<bollard::query_parameters::RemoveContainerOptions>,
                )
                .await
                .unwrap();

            docker
                .prune_volumes(None::<PruneVolumesOptions>)
                .await
                .unwrap();
            docker.prune_build(None::<PruneBuildOptions>).await.unwrap();
            docker
                .prune_images(None::<PruneImagesOptions>)
                .await
                .unwrap();
        });
        // Delete index files
        let name = self.qlever_file.data.get("NAME").unwrap().as_str();
        let dir = format!("~/extern/data/{name}");
        let _ = remove_dir_all(dir);

        Ok(())
    }

    /// Run a test query on QLever
    pub fn run_test_query(&mut self, query: &str) -> Result<u128, Box<dyn Error>> {
        // Run query
        let response = self.do_query_request(query);
        // Write results to log
        let result: Result<u128, Box<dyn Error>> = match response {
            Ok((time, rows, columns)) => {
                info!(
                    "Query: {query}\n Duration: {time} Result Size: Columns {columns} Rows {rows}"
                );
                Ok(time)
            }
            Err(e) => {
                error!("Query: {query} failed\n{e}");
                Err(e)
            }
        };

        result
    }

    /// Make a request to the QLever server with a query
    fn do_query_request(&mut self, query: &str) -> Result<(u128, usize, usize), Box<dyn Error>> {
        let port = self
            .qlever_file
            .server
            .get("PORT")
            .ok_or("Failed reading Port from file")?
            .as_str();
        // Create Headers
        let mut headers = header::HeaderMap::new();
        headers.insert("Accept", "application/qlever-results+json".parse().unwrap());
        headers.insert("Content-type", "application/sparql-query".parse().unwrap());

        // Create reqwest client
        let client = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(Duration::from_secs(600))
            .build()?;
        let query = query.to_string();
        // Do Post request with query
        let res = client
            .post(format!("http://127.0.0.1:{port}"))
            .headers(headers)
            .body(query)
            .send()?
            .error_for_status();

        // Check for http Errors
        if let Err(err) = res {
            return Err(err.into());
        }

        // Parse Result into JsonResult
        let text = res.unwrap().text().unwrap();
        let result: JsonResult = serde_json::from_str(&text)?;

        // Convert time from "Xms" into integer
        let time: u128 = result
            .time
            .total
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse::<u128>()?;

        Ok((
            time,
            result.runtime.query_execution_tree.result_rows,
            result.runtime.query_execution_tree.result_cols,
        ))
    }
}

/// Datastruct for converted QLeverfile
#[derive(Deserialize, Clone)]
pub struct QleverFile {
    pub data: HashMap<String, String>,
    pub index: HashMap<String, String>,
    pub server: HashMap<String, String>,
}

impl QleverFile {
    /// Replaces variables inside the QLeverfile with the corresponding value
    pub fn replace_internal_variables(&mut self) {
        trace!("Replacing internal variables in QLeverFile");
        let regex = Regex::new(r"\$\{(?<prefix>\w+:)?(?<key>\w+)}").unwrap();
        // Iterate over Data
        self.data = self
            .data
            .iter()
            .map(|(orig_key, value)| {
                let mut changed = value.clone();
                while regex.is_match(changed.as_str()) {
                    changed = regex
                        .replace_all(changed.as_str(), |cap: &Captures| {
                            let prefix = cap.name("prefix");
                            let key = cap.name("key").unwrap().as_str();
                            match prefix {
                                Some(prefix) if prefix.as_str() == "data:" => {
                                    self.data.get(key).unwrap()
                                }
                                Some(prefix) if prefix.as_str() == "index:" => {
                                    self.index.get(key).unwrap()
                                }
                                Some(prefix) if prefix.as_str() == "server:" => {
                                    self.server.get(key).unwrap()
                                }
                                None | Some(_) => self.data.get(key).unwrap(),
                            }
                        })
                        .to_string();
                }
                (orig_key.to_string(), changed)
            })
            .collect();
        // Iterate over Index
        self.index = self
            .index
            .iter()
            .map(|(orig_key, value)| {
                let mut changed = value.clone();
                while regex.is_match(changed.as_str()) {
                    changed = regex
                        .replace_all(changed.as_str(), |cap: &Captures| {
                            let prefix = cap.name("prefix");
                            let key = cap.name("key").unwrap().as_str();
                            match prefix {
                                Some(prefix) if prefix.as_str() == "data:" => {
                                    self.data.get(key).unwrap()
                                }
                                Some(prefix) if prefix.as_str() == "index:" => {
                                    self.index.get(key).unwrap()
                                }
                                Some(prefix) if prefix.as_str() == "server:" => {
                                    self.server.get(key).unwrap()
                                }
                                None | Some(_) => self.index.get(key).unwrap(),
                            }
                        })
                        .to_string();
                }
                (orig_key.to_string(), changed)
            })
            .collect();
        // Iterate over Server
        self.server = self
            .server
            .iter()
            .map(|(orig_key, value)| {
                let mut changed = value.clone();
                while regex.is_match(changed.as_str()) {
                    changed = regex
                        .replace_all(changed.as_str(), |cap: &Captures| {
                            let prefix = cap.name("prefix");
                            let key = cap.name("key").unwrap().as_str();
                            match prefix {
                                Some(prefix) if prefix.as_str() == "data:" => {
                                    self.data.get(key).unwrap()
                                }
                                Some(prefix) if prefix.as_str() == "index:" => {
                                    self.index.get(key).unwrap()
                                }
                                Some(prefix) if prefix.as_str() == "server:" => {
                                    self.server.get(key).unwrap()
                                }
                                None | Some(_) => self.server.get(key).unwrap(),
                            }
                        })
                        .to_string();
                }
                (orig_key.to_string(), changed)
            })
            .collect();
    }
}

/// Response deserialization struct
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
    query_execution_tree: JsonQueryExecTree,
}

#[derive(Deserialize)]
struct JsonQueryExecTree {
    result_cols: usize,
    result_rows: usize,
}

/// Utility function for executing commands in the right directory
fn command_assist(
    command_str: &str,
    args: &[&str],
    current_dir: &str,
) -> Result<(), Box<dyn Error>> {
    let command = Command::new(command_str)
        .args(args)
        .current_dir(current_dir)
        .output()
        .unwrap_or_else(|e| {
            let error = "Failed executing command ".to_string() + command_str + " " + args.join(" ").as_str();
            error!("{error}:{e}");
            panic!("{error}")
        }
        );
    info!(
        "executed_command: {}",
        command_str.to_string() + " " + args.join(" ").as_str()
    );
    info!("status: {}", &command.status);
    info!("stdout:\n{}", String::from_utf8_lossy(&command.stdout));
    info!("stderr:\n{}", String::from_utf8_lossy(&command.stderr));
    if command.status.success() {
        Ok(())
    } else {
        Err(format!(
            "Command failed status code: {0}\nCommand: {1} {2:?}",
            command.status, command_str, args
        )
        .into())
    }
}

/// Sanitize QLeverfile so it can be Deserialized without problem
fn sanitize_toml(string: String) -> String {
    // Regex Setup
    let comments = Regex::new(r"#\s.*\n?").unwrap();
    let jsons = Regex::new(r"=\s(.*)").unwrap();
    // Remove Comments
    let mut new_toml = comments.replace_all(string.as_str(), "").to_string();
    // Replace double quotes with single quotes
    new_toml = new_toml.as_str().replace("\"", "\'").to_string();
    // Wrap properties in double quotes
    new_toml = jsons
        .replace_all(new_toml.as_str(), |caps: &Captures| {
            let word = caps[1].to_string();
            format!("= \"{word}\"")
        })
        .to_string();
    new_toml.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_eq;

    fn test_file() -> String {
        include_str!("test-data/test_qleverfile.toml").to_string()
    }

    #[test]
    fn test_sanitize_toml() {
        let file = test_file();
        let sanitized = sanitize_toml(file);

        // Removed all comments
        assert!(!sanitized.contains("#"));
        // Replaced double quotes with single quotes
        assert!(sanitized.contains("'Test string with double quotes"));
        assert!(sanitized.contains("{ 'value_1': 'test', 'value_2': 'testing' }"));
        // Wrapped properties in double quotes
        let properties = Regex::new(r#"\S*\s*=\s[^"].*[^"]$"#).unwrap();
        assert!(!properties.is_match(&sanitized))
    }

    #[test]
    fn test_qlever_file_internal_replace() {
        let file = test_file();
        let sanitized = sanitize_toml(file);
        let mut qleverfile = toml::from_str::<QleverFile>(sanitized.as_str()).unwrap();
        qleverfile.replace_internal_variables();

        assert_eq!(
            qleverfile.data.get("TEST_VARIABLE_REPLACE"),
            Some(&"exec something-test".to_string())
        );
        assert_eq!(
            qleverfile.data.get("TEST_NESTED_REPLACE"),
            Some(&"exec something else; exec something-test".to_string())
        );
        assert_eq!(
            qleverfile.index.get("TEST_REPLACE_OTHER"),
            Some(&"testing test".to_string())
        );
    }
}
