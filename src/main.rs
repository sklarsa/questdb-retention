use futures::executor::block_on;
use postgres::row::Row;
use postgres::{Client, NoTls};
use prompts::{text::TextPrompt, Prompt};
use std::collections::HashMap;
use std::fmt::{self};
use std::str::FromStr;

#[derive(Debug)]
enum RetentionPeriodError {
    InvalidPartitionBy(PartitionBy),
    InvalidAmount(i64),
}

impl fmt::Display for RetentionPeriodError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RetentionPeriodError::InvalidPartitionBy(x) => write!(f, "invalid PartitionBy {}", x),
            RetentionPeriodError::InvalidAmount(x) => write!(f, "invalid Amount {}", x),
        }
    }
}

#[derive(Debug, Clone)]
struct RetentionPeriod {
    amount: i64,
    partition_by: PartitionBy,
}

fn new_retention_period(
    amount: i64,
    partition_by: PartitionBy,
) -> Result<RetentionPeriod, RetentionPeriodError> {
    if partition_by == PartitionBy::NONE {
        return Err(RetentionPeriodError::InvalidPartitionBy(partition_by));
    }

    if amount <= 0 {
        return Err(RetentionPeriodError::InvalidAmount(amount));
    }

    Ok(RetentionPeriod {
        amount: amount,
        partition_by: partition_by,
    })
}

#[derive(Debug, PartialEq, Clone)]
enum PartitionBy {
    NONE,
    YEAR,
    MONTH,
    DAY,
    HOUR,
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
            "NONE" => Ok(PartitionBy::NONE),
            "YEAR" => Ok(PartitionBy::YEAR),
            "MONTH" => Ok(PartitionBy::MONTH),
            "DAY" => Ok(PartitionBy::DAY),
            "HOUR" => Ok(PartitionBy::HOUR),
            _ => Err(()),
        }
    }
}

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

        if table.partition_by != PartitionBy::NONE {
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
                    )
                }
                Ok(None) => println!("You typed nothing"),
                Err(e) => println!("error: {}", e),
            }
        }
    }
}
