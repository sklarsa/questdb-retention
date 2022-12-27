use chrono::{DateTime, Duration, Utc};
use futures::executor::block_on;
use postgres::row::Row;
use postgres::{Client, NoTls};
use prompts::{text::TextPrompt, Prompt};
use serde::{Deserialize, Serialize};
use serde_yaml;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self};
use std::fs::File;
use std::str::FromStr;

#[derive(Debug)]
enum RetentionPeriodError {
    UnsupportedPartitionBy(PartitionBy),
    InvalidPartitionBy(PartitionBy),
    InvalidAmount(i64),
}

impl Error for RetentionPeriodError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl fmt::Display for RetentionPeriodError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RetentionPeriodError::UnsupportedPartitionBy(x) => {
                write!(f, "unsupported PartitionBy {}", x)
            }
            RetentionPeriodError::InvalidPartitionBy(x) => write!(f, "invalid PartitionBy {}", x),
            RetentionPeriodError::InvalidAmount(x) => write!(f, "invalid Amount {}", x),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetentionPeriod {
    amount: i64,
    partition_by: PartitionBy,
}

fn new_retention_period(
    amount: i64,
    partition_by: PartitionBy,
) -> Result<RetentionPeriod, RetentionPeriodError> {
    if partition_by == PartitionBy::None {
        return Err(RetentionPeriodError::InvalidPartitionBy(partition_by));
    }

    if amount <= 0 {
        return Err(RetentionPeriodError::InvalidAmount(amount));
    }

    Ok(RetentionPeriod {
        amount,
        partition_by,
    })
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
enum PartitionBy {
    None,
    Year,
    Month,
    Day,
    Hour,
}

impl fmt::Display for PartitionBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for PartitionBy {
    type Err = ();

    fn from_str(input: &str) -> Result<PartitionBy, Self::Err> {
        match input {
            "NONE" => Ok(PartitionBy::None),
            "YEAR" => Ok(PartitionBy::Year),
            "MONTH" => Ok(PartitionBy::Month),
            "DAY" => Ok(PartitionBy::Day),
            "HOUR" => Ok(PartitionBy::Hour),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Table {
    name: String,
    partition_by: PartitionBy,
}

fn row_to_table(r: &Row) -> Table {
    Table {
        name: r.get("name"),
        partition_by: PartitionBy::from_str(r.get("partitionBy")).unwrap(),
    }
}

fn parse_config_file(f: File) -> Result<Vec<Table>, serde_yaml::Error> {
    serde_yaml::from_reader(f)
}

fn get_timestamp_col(client: &mut Client, table: &String) -> Result<String, postgres::Error> {
    let query = format!(
        "SELECT designatedTimestamp FROM tables() WHERE name='{}'",
        table
    );
    match client.query_one(&query, &[]) {
        Ok(r) => Ok(r.get("designatedTimestamp")),
        Err(e) => Err(e),
    }
}

fn get_oldest_timestamp(p: RetentionPeriod) -> Result<DateTime<Utc>, RetentionPeriodError> {
    let now = Utc::now();
    match p.partition_by {
        PartitionBy::Day => Ok(now - Duration::days(p.amount)),
        PartitionBy::Hour => Ok(now - Duration::hours(p.amount)),
        PartitionBy::None => Err(RetentionPeriodError::UnsupportedPartitionBy(p.partition_by)),
        // TODO: handle months and years, but chronos does not support thm...
        _ => Err(RetentionPeriodError::UnsupportedPartitionBy(p.partition_by)),
    }
}

fn run(client: &mut Client, table: &String, p: RetentionPeriod) -> Result<(), Box<dyn Error>> {
    // Get timestamp column
    let mut timestamp_col = String::from("");
    match get_timestamp_col(client, table) {
        Ok(t) => timestamp_col = t,
        Err(e) => return Err(Box::new(e)),
    }

    // Get oldest timestamp to keep
    let mut timestamp: DateTime<Utc> = DateTime::default();
    match get_oldest_timestamp(p) {
        Ok(d) => timestamp = d,
        Err(e) => return Err(Box::new(e)),
    }

    // Drop all partitions earlier than that timestamp
    let query = format!(
        "ALTER TABLE {} DROP PARTITION WHERE {} < to_timestamp('{}', 'yyyy-MM-dd:HH:mm:ss')",
        table, timestamp_col, timestamp
    );
    match client.execute(&query, &[]) {
        Ok(n) => {
            println!("{} rows deleted", n);
            return Ok(());
        }
        Err(e) => Err(Box::new(e)),
    }
}

fn main() {
    let mut periods = HashMap::new();
    let mut client =
        Client::connect("host=localhost user=admin password=quest port=8812", NoTls).unwrap();
    println!("tables:");
    for row in client.query("tables()", &[]).unwrap() {
        let table = row_to_table(&row);
        println!(
            "name: '{}' is partitioned by '{}'",
            table.name, table.partition_by
        );

        if table.partition_by != PartitionBy::None {
            let mut prompt = TextPrompt::new(format!(
                "how many {}s do you want to retain?",
                table.partition_by
            ))
            .with_validator(|s| -> Result<(), String> {
                match s.parse::<i32>() {
                    Ok(..) => Ok(()),
                    Err(e) => Err(format!("{}", e)),
                }
            });

            match block_on(prompt.run()) {
                Ok(Some(a)) => {
                    let p = new_retention_period(a.parse::<i64>().unwrap(), table.partition_by)
                        .unwrap();
                    periods.insert(table.name.clone(), p.clone());
                    println!(
                        "Added retention period of {} {}s for {}",
                        p.amount, p.partition_by, table.name
                    );
                }
                Ok(None) => println!("You typed nothing"),
                Err(e) => println!("error: {}", e),
            }
        }
    }
}
