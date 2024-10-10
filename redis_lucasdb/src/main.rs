use lucasdb::errors::Result;
use std::{collections::HashMap, sync::Mutex, time::Duration};

use lucasdb::options::EngineOptions;
use redis_lucasdb::types::RedisLucasDb;
const SERVER_ADDR: &str = "0.0.0.0:56379";

type CmdHandler = dyn Fn(&mut redcon::Conn, Vec<Vec<u8>>, &Mutex<RedisLucasDb>);

fn init_cmd_handler() -> HashMap<&'static str, Box<CmdHandler>> {
    let mut supported_commands = HashMap::new();

    {
        supported_commands.insert("set", Box::new(set) as Box<CmdHandler>);
        supported_commands.insert("get", Box::new(get) as Box<CmdHandler>);
        supported_commands.insert("hset", Box::new(hset) as Box<CmdHandler>);
        supported_commands.insert("sadd", Box::new(sadd) as Box<CmdHandler>);
        supported_commands.insert("lpush", Box::new(lpush) as Box<CmdHandler>);
        supported_commands.insert("rpush", Box::new(rpush) as Box<CmdHandler>);
        supported_commands.insert("zadd", Box::new(zadd) as Box<CmdHandler>);
    }

    supported_commands
}

fn main() -> Result<()> {
    let rds = Mutex::new(RedisLucasDb::new(EngineOptions::default())?);

    let mut lucasdb_server = redcon::listen(SERVER_ADDR, rds).expect("failed to listen addr");

    lucasdb_server.command = Some(|conn, rds, args| {
        let name = String::from_utf8_lossy(&args[0]).to_lowercase();

        let supported_commands = init_cmd_handler();

        match supported_commands.get(name.as_str()) {
            Some(handler) => handler(conn, args, rds),
            None => conn.write_error("ERR unknown command"),
        }
    });

    println!("lucasdb server serving at {}", lucasdb_server.local_addr());
    lucasdb_server.serve().expect("serve error");
    Ok(())
}

fn set(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisLucasDb>) {
    println!("set");
    if args.len() != 3 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let rds = rds.lock().unwrap();
    let res = rds.set(
        &String::from_utf8_lossy(&args[1]),
        Duration::ZERO,
        &String::from_utf8_lossy(&args[2]),
    );

    if res.is_err() {
        conn.write_error(&res.err().unwrap().to_string());
        return;
    }

    conn.write_string("OK");
}

fn get(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisLucasDb>) {
    println!("get");

    if args.len() != 2 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let rds = rds.lock().unwrap();
    let res = rds.get(&String::from_utf8_lossy(&args[1]));

    match res {
        Ok(val) => {
            conn.write_string(val.unwrap().as_str());
        }
        Err(e) => conn.write_error(e.to_string().as_str()),
    }
}

fn hget(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisLucasDb>) {}

fn hset(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisLucasDb>) {
    if args.len() != 3 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let rds = rds.lock().unwrap();
    let key = String::from_utf8_lossy(&args[1]);
    let field = String::from_utf8_lossy(&args[2]);
    let value = String::from_utf8_lossy(&args[3]);
    match rds.hset(&key, &field, &value) {
        Ok(val) => conn.write_integer(val as i64),
        Err(e) => conn.write_error(e.to_string().as_str()),
    }
}

fn sadd(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisLucasDb>) {
    if args.len() != 3 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let rds = rds.lock().unwrap();
    let key = String::from_utf8_lossy(&args[1]);
    let member = String::from_utf8_lossy(&args[2]);
    match rds.sadd(&key, &member) {
        Ok(val) => conn.write_integer(val as i64),
        Err(e) => conn.write_error(e.to_string().as_str()),
    }
}

fn lpush(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisLucasDb>) {
    if args.len() != 3 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let rds = rds.lock().unwrap();
    let key = String::from_utf8_lossy(&args[1]);
    let value = String::from_utf8_lossy(&args[2]);
    match rds.lpush(&key, &value) {
        Ok(val) => conn.write_integer(val as i64),
        Err(e) => conn.write_error(e.to_string().as_str()),
    }
}

fn rpush(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisLucasDb>) {
    if args.len() != 3 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let rds = rds.lock().unwrap();
    let key = String::from_utf8_lossy(&args[1]);
    let value = String::from_utf8_lossy(&args[2]);
    match rds.rpush(&key, &value) {
        Ok(val) => conn.write_integer(val as i64),
        Err(e) => conn.write_error(e.to_string().as_str()),
    }
}

fn zadd(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisLucasDb>) {
    if args.len() != 4 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let rds = rds.lock().unwrap();
    let key = String::from_utf8_lossy(&args[1]);
    let score = String::from_utf8_lossy(&args[2]);
    let member = String::from_utf8_lossy(&args[3]);
    match rds.zadd(&key, score.parse().unwrap(), &member) {
        Ok(val) => conn.write_integer(val as i64),
        Err(e) => conn.write_error(e.to_string().as_str()),
    }
}
