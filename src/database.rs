use rusqlite::Connection;

pub fn open_database(path: &str) -> rusqlite::Result<Connection> {
    Connection::open(path)    
}