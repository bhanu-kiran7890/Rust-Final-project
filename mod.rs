use reqwest::blocking::Client;
use std::thread::sleep;
use std::time::Duration;

//Collecting data from API
pub fn fetch_data_from_api(url: &str, attempts: usize) -> Result<String, String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| e.to_string())
        .unwrap_or_else(|err| {
            panic!("Failed to create client: {}", err);
        });

    for _ in 0..attempts {
        match client.get(url).send() {
            Ok(response) => {
                if response.status().is_success() {
                    match response.text() {
                        Ok(data) => return Ok(data),
                        Err(e) => println!("Failed to parse response: {}", e),
                    }
                } else {
                    println!("Failed to fetch data. Status: {}", response.status());
                }
            }
            Err(e) => {
                println!("Failed to send request: {}", e);
                sleep(Duration::from_secs(1));
            }
        }
    }
    Err("Exceeded retry limit".to_string())
}
