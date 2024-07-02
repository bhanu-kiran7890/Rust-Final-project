use polars::prelude::*;
use postgres::{Client, NoTls};
use project::{fet_data_from_csv_file, data_transformation, create_table, insert_data};
fn main() {
    let file_path = "dataset.csv";
    match fet_data_from_csv_file(file_path, 2) {
        Ok(data) => println!("{:?}", data),
        Err(e) => println!("Failed to fetch csv file data: {}", e),
    }

    let df = data_transformation();
    println!("{:?}", df);

    let df = df.unwrap();
    let df1 = df.drop_nulls::<String>(None);
    println!("{:?}", df1);
    for col in df.get_columns() {
        match col.dtype() {
            DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64 => {
                println!("{:?}", col.name());
            }
            _ => {}
        }
    }

    let mut client = match Client::connect(
        "host=localhost user=postgres password=Password@1234",
        NoTls,
    ) {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            return;
        }
    };
    println!("Database connected");

    match create_table(&mut client) {
        Ok(_) => println!("Table created successfully"),
        Err(e) => eprintln!("Failed to create table: {}", e),
    }

    match insert_data(&mut client, &df) {
        Ok(_) => println!("Data inserted into tables"),
        Err(e) => eprintln!("Failed to insert data: {}", e),
    }
}
