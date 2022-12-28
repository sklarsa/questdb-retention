use chrono::{DateTime, Duration, Utc};
use clap::Parser;
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
    InvalidAmount(i64),
    InvalidPartitionBy(PartitionBy),
    UnsupportedPartitionBy(PartitionBy),
    UnknownPartitionBy(String),
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
            RetentionPeriodError::UnknownPartitionBy(x) => {
                write!(f, "unknown PartitionBy value: '{}'", x)
            }
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
    type Err = RetentionPeriodError;

    fn from_str(input: &str) -> Result<PartitionBy, Self::Err> {
        match input {
            "NONE" => Ok(PartitionBy::None),
            "YEAR" => Ok(PartitionBy::Year),
            "MONTH" => Ok(PartitionBy::Month),
            "DAY" => Ok(PartitionBy::Day),
            "HOUR" => Ok(PartitionBy::Hour),
            _ => Err(RetentionPeriodError::UnknownPartitionBy(input.to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Table {
    name: String,
    partition_by: PartitionBy,
}

fn row_to_table(r: &Row) -> Result<Table, RetentionPeriodError> {
    match PartitionBy::from_str(r.get("partitionBy")) {
        Ok(p) => Ok(Table {
            name: r.get("name"),
            partition_by: p,
        }),
        Err(e) => Err(e),
    }
}

fn get_timestamp_col(client: &mut Client, table: &str) -> Result<String, postgres::Error> {
    let query = format!(
        "SELECT designatedTimestamp FROM tables() WHERE name='{}'",
        table
    );
    Ok(client.query_one(&query, &[])?.get("designatedTimestamp"))
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

fn run(client: &mut Client, table: &str, p: RetentionPeriod) -> Result<u64, Box<dyn Error>> {
    // Get timestamp column
    let timestamp_col = get_timestamp_col(client, table)?;

    // Get oldest timestamp to keep
    let timestamp: DateTime<Utc> = get_oldest_timestamp(p)?;

    // Drop all partitions earlier than that timestamp
    let query = format!(
        "ALTER TABLE {} DROP PARTITION WHERE {} < to_timestamp('{}', 'yyyy-MM-dd:HH:mm:ss')",
        table, timestamp_col, timestamp
    );
    Ok(client.execute(&query, &[])?)
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "")]
    config_path: String,

    #[arg(short, long)]
    interactive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    tables: HashMap<String, i64>,
    conn_str: String,
}

fn parse_config(path: &str) -> Result<Config, String> {
    match File::open(path) {
        Ok(f) => match serde_yaml::from_reader::<File, Config>(f) {
            Ok(c) => Ok(c),
            Err(e) => Err(e.to_string()),
        },
        Err(e) => Err(e.to_string()),
    }
}

fn run_interactive(client: &mut Client) -> Result<(), String> {
    let mut prompt = TextPrompt::new(format!("which table do you want to truncate?"));

    match block_on(prompt.run()) {
        Ok(Some(t)) => {
            for row in client.query("tables()", &[]).unwrap() {
                if String::from_str(row.get("name")).unwrap() == t {
                    let table = row_to_table(&row).unwrap();
                    if table.partition_by == PartitionBy::None {
                        return Err(RetentionPeriodError::InvalidPartitionBy(table.partition_by)
                            .to_string());
                    }

                    let mut prompt = TextPrompt::new(format!(
                        "how many {}s do you want to retain?",
                        table.partition_by
                    ))
                    .with_validator(|s| -> Result<(), String> {
                        match s.parse::<i32>() {
                            Ok(..) => Ok(()),
                            Err(e) => Err(format!("error: {}", e)),
                        }
                    });

                    match block_on(prompt.run()) {
                        Ok(Some(a)) => {
                            let p =
                                new_retention_period(a.parse::<i64>().unwrap(), table.partition_by)
                                    .unwrap();

                            println!("Deleting old partitions...");
                            match run(client, &table.name, p) {
                                Ok(d) => println!("deleted {} rows", d),
                                Err(e) => return Err(e.to_string()),
                            }
                        }
                        Ok(None) => {
                            return Err(String::from("You typed nothing"));
                        }
                        Err(e) => return Err(e.to_string()),
                    }
                }
            }
            return Err(String::from(format!("table not found '{}'", t)));
        }

        Ok(None) => {
            return Err(String::from("no table supplied... exiting"));
        }
        Err(e) => return Err(e.to_string()),
    }
}

fn run_from_config(client: &mut Client, tables: HashMap<String, i64>) -> Result<(), String> {
    for t in tables.keys() {
        match run_one(client, t.clone(), tables.get(t).unwrap()) {
            Ok(m) => println!("{}", m),
            Err(e) => println!("{}", e),
        }
    }
    Ok(())
}

fn run_one(client: &mut Client, table: String, amount: &i64) -> Result<String, String> {
    match client.query_one("SELECT * FROM tables() WHERE name=$1", &[&table]) {
        Ok(r) => match row_to_table(&r) {
            Ok(t) => match new_retention_period(*amount, t.partition_by) {
                Ok(p) => match run(client, &t.name, p) {
                    Ok(n) => Ok(format!("{} rows deleted from {}", n, t.name)),
                    Err(e) => Err(e.to_string()),
                },
                Err(e) => Err(e.to_string()),
            },
            Err(e) => Err(e.to_string()),
        },
        Err(e) => Err(e.to_string()),
    }
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    let mut conn_str = String::from("host=localhost user=admin password=quest port=8812");
    let mut tables: HashMap<String, i64> = HashMap::new();
    if args.config_path != "" {
        match parse_config(&args.config_path) {
            Ok(c) => {
                conn_str = c.conn_str;
                tables = c.tables;
            }
            Err(e) => return Err(e),
        }
    }

    let mut client = Client::connect(&conn_str, NoTls).unwrap();

    if args.interactive {
        return run_interactive(&mut client);
    }

    if args.config_path != "" {
        return run_from_config(&mut client, tables);
    }

    Err(String::from(
        "must choose interactive mode or pass a config file",
    ))
}
