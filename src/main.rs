mod keyval;

use std::time;
use std::thread;

fn main() {
    let duration = time::Duration::from_millis(1000);
    let mut store = keyval::key_val_store::KVStore::new("database".to_string());
    store.run();
    store.insert("Chicken".to_string(), "Buggy".to_string());
    store.insert("Biryani2".to_string(), "Soorkie".to_string());

    if let Some(val) = store.get("Biryani2".to_string()){
        println!("Ans: {}", val);
    }

    thread::sleep(duration);

}
