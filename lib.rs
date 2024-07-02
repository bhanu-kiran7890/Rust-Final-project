use csv::Reader;
use std::error::Error;
use std::fs::File;
use std::thread::sleep;
use std::time::Duration;
mod data_ingestion;
use polars_core::prelude::*;
use polars_io::prelude::*;
use postgres::{Client};
pub fn fet_data_from_csv_file(
    file_path: &str,
    retries: usize,
) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    let mut attempts = 0;
    while attempts < retries {
        match File::open(file_path) {
            Ok(file) => {
                let mut rdr = Reader::from_reader(file);
                let mut records = Vec::new();
                for result in rdr.records() {
                    let record = result?;
                    records.push(record.iter().map(|s| s.to_string()).collect());
                }
                return Ok(records);
            }
            Err(e) => {
                println!("Error reading CSV file: {}", e);
                attempts += 1;
                sleep(Duration::from_secs(2));
            }
        }
    }
    Err(Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        "Exceeded retry limit",
    )))
}

pub fn data_transformation() -> PolarsResult<DataFrame> {
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some("dataset.csv".into()))?
        .finish()?;

    let gender = df["gender"].clone();
    let age = df["age"].clone();
    let hypertension = df["hypertension"].clone();
    let heart_disease = df["heart_disease"].clone();
    let smoking_history = df["smoking_history"].clone();
    let bmi = df["bmi"].clone();
    let hba1c_level = df["HbA1c_level"].clone();

    let df = DataFrame::new(vec![
        gender,
        age,
        hypertension,
        heart_disease,
        smoking_history,
        bmi,
        hba1c_level,
    ])?;

    Ok(df)
}


pub fn create_table(client: &mut Client) -> Result<(), Box<dyn Error>> {
    client.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS dataset (
            id      SERIAL PRIMARY KEY,
            gender    TEXT NOT NULL,
            age      TEXT,
            hypertension TEXT,
            heart_disease TEXT,
            smoking_history TEXT,
            bmi TEXT,
            HbA1c_level TEXT
        )
        ",
    )?;
    Ok(())
}

pub fn insert_data(client: &mut Client, df: &DataFrame) -> Result<(), Box<dyn Error>> {
    for row in df.iter() {
        let mut gender = String::new();
        let mut age = String::new();
        let mut hypertension = String::new();
        let mut heart_disease = String::new();
        let mut smoking_history = String::new();
        let mut bmi = String::new();
        let mut hba1c_level = String::new();

        for (idx, val) in row.iter().enumerate() {
            match idx {
                0 => gender = val.to_string(),
                1 => age = val.to_string(),
                2 => hypertension = val.to_string(),
                3 => heart_disease = val.to_string(),
                4 => smoking_history = val.to_string(),
                5 => bmi = val.to_string(),
                6 => hba1c_level = val.to_string(),
                _ => (),
            }
        }

        client.query(
            "INSERT INTO dataset (gender, age, hypertension, heart_disease, smoking_history, bmi, HbA1c_level) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &gender,
                &age,
                &hypertension,
                &heart_disease,
                &smoking_history,
                &bmi,
                &hba1c_level,
            ],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_fet_data_from_csv_file() {
        let file_path = "test.csv";
        let data = "gender,age,hypertension,heart_disease,smoking_history,bmi,HbA1c_level\n\
                    Male,34,0,0,Non-smoker,23.4,5.1\n\
                    Female,45,1,0,Ex-smoker,27.1,6.2";

        let mut file = File::create(file_path).expect("Could not create file");
        file.write_all(data.as_bytes()).expect("Could not write to file");

        let result = fet_data_from_csv_file(file_path, 1);
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], vec!["Male", "34", "0", "0", "Non-smoker", "23.4", "5.1"]);
    }
}
