use std::{path::Path, env};

use rusqlite::{Connection, Result};

struct Pair {
    key: String,
    value: String
}

pub fn open_database(path: &str) -> Result<Connection> {
    Connection::open(path)    
}

pub fn create_database(path: &str) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute("DROP TABLE IF EXISTS pairs; CREATE TABLE pairs(key TEXT, value TEXT);", ())?;
    Ok(conn)
}

pub fn split_database(source: &str, output_dir: &str, output_pattern: fn(u32) -> String, m: u32) -> Result<Vec<String>> {
    let conn = open_database(source)?;
    let mut stmt = conn.prepare("SELECT * FROM pairs;")?;
    let pair_iter = stmt.query_map([], |row| {
        Ok(Pair {
            key: row.get(0)?,
            value: row.get(1)?
        })
    })?;

    let base_path = Path::new(output_dir);
    let paths: Vec<String> = (0..m-1).map(|n| {
        let str = output_pattern(n);
        let filename = Path::new(str.as_str());
        let path = base_path.join(filename);
        String::from(path.to_str().unwrap())
    }).collect();

    let mut output_dbs: Vec<Connection> = (0..m-1).map(|n| -> Connection {
        let filename = format!("{}", output_pattern(n));
        let path = env::join_paths([base_path, Path::new(filename.as_str())]).unwrap();
        create_database(path.to_str().unwrap()).unwrap()
    }).collect();

    let mut i: usize = 0;
    for pair in pair_iter {
        let p = pair.unwrap();
        if i > m as usize { i = 0; }
        let conn = &output_dbs[i];
        conn.execute("INSERT INTO pairs ({}, {});", (p.key, p.value))?;
    }

    Ok(Vec::new())
}
