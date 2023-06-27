pub mod key_val_store {
    use std::collections::HashMap;
    use std::path::Path;
    use std::path::PathBuf;
    use std::fs::OpenOptions;
    use std::io::prelude::*;
    use std::str::FromStr;
    use std::fs;
    use glob::glob;
    use std::io::{ self, BufReader, Read };
    use std::collections::HashSet;
    use std::mem::{drop};
    
    // Threading
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::thread;
    use std::time;

    const PREFIX_FILE_NAME: &str = "soorkie";
    const PREFIX_TEMP_NAME: &str = "soorkie_temp";
    const CLEANUP_JOB_MARGIN: u32 = 2;
    const CLEANUP_JOB_INTERVAL : time::Duration= time::Duration::from_millis(1000 * 60 * 30);
    const MAX_CHUNK_SIZE_BYTES: u64 = 1 * 1024;
    const MAX_HASHSET_SIZE : u32 = 1_000_000;

    pub enum ErrorKind {
        UnableToReadPreviousMaps,
        FileReadFailed,
        FileSeekFailed,
        FileCopyFailed,
        FileDeleteFailed,
        DataByteLengthSmall,
        DataByteCorrupted,
        DataByteLenDecodeFailed,
        DataByteKeyDecodeFailed,
        DataByteValDecodeFailed,
        EncodeDataTooBig,
    }

    #[derive(Clone)]
    pub struct DiskPointer {
        file: u32,
        temp: bool,
        offset: usize,
    }

    #[derive(Clone)]
    pub struct KVStore {
        pub file_root: String,
        hash_map: Arc<Mutex<HashMap<String, DiskPointer>>>,
        working_file: u32,
        working_offset: usize,
        locked_data : Arc<Mutex<u32>>,
    }

    impl KVStore {
        pub fn new(file_root: String) -> KVStore {
            let previous_state = get_latest(file_root.clone(), PREFIX_FILE_NAME.to_string());
            let (mut previous_maps, mut previous_offset) = match
                get_maps(&file_root, &PREFIX_FILE_NAME.to_string(), &previous_state)
            {
                Ok(val) => val,
                Err(error) => {
                    (HashMap::new(), 0usize)
                }
            };

            KVStore {
                file_root: file_root,
                hash_map: Arc::new(Mutex::new(previous_maps)),
                working_file: previous_state.clone(),
                working_offset: previous_offset,
                locked_data: Arc::new(Mutex::new(0u32)),
            }
        }

        pub fn run(&self) {
            let mut me = self.clone();
            thread::spawn(move || me.core_job());
        }

        pub fn debug_mutate(&mut self) {
            let mut lock = self.locked_data.lock().unwrap();
            *lock = 1;
        }

        fn core_job(&mut self) -> Result<(), ErrorKind> {
            let duration = CLEANUP_JOB_INTERVAL;
            loop {
                // Check if we have to clean up
                let (mut min, mut max) = get_workspace_bounds(&self.file_root, &PREFIX_FILE_NAME.to_string());
                max -= CLEANUP_JOB_MARGIN;
                if min < max  {
                    // Do job merge 
                    let mut prev_logs : HashMap<String, u32> = HashMap::new();
                    let mut write_hash : HashMap<String, String> = HashMap::new();
                    let mut prev_logs_size : u32 = 0;
                    let mut write_hash_size : u32 = 0;
                    let mut target_page : u32 = max.clone();
                    let mut i = max;
                    loop {
                        // load key, values, total_size one by one from current file
                        let full_path = get_full_path(&self.file_root, &PREFIX_FILE_NAME.to_string(), &i);

                        let file = match fs::File::open(full_path) {
                            Ok(res) => res,
                            Err(_error) => {
                                return Err(ErrorKind::FileReadFailed);
                            }
                        };
                        let mut reader = BufReader::new(file);
                        let mut size_buffer = [0u8; 2];


                        loop {
                            match reader.read_exact(&mut size_buffer) {
                                Ok(val) => val,
                                Err(_error) => {
                                    break;
                                }
                            }

                            let total_size = ((size_buffer[0] as u16) << 8) + (size_buffer[1] as u16);
                            match reader.seek_relative(-2) {
                                Ok(val) => val,
                                Err(_error) => {
                                    break;
                                }
                            }

                            let mut data_buffer : Vec<u8> = vec![0u8; total_size as usize];
                            match reader.read_exact(&mut data_buffer) {
                                Ok(val) => val,
                                Err(_error) => {
                                    break;
                                }
                            }
                
                            let key_val = decode(data_buffer)?;
                            let key = key_val.0;
                            let val = key_val.1;

                            // if it in hashset, next
                            if let Some(current_val) = prev_logs.get(&key) {
                                // has key
                                if current_val > &i {
                                    // current one is important, dont change
                                    continue;
                                }
                                // println!("{}", current_val);
                            }
                        
                            // Add to hash set, update hash set size
                            prev_logs.insert(key.clone(), i);
                            prev_logs_size += 1;

                            // Add key to write stack, update write stack size
                            if !write_hash.contains_key(&key) {
                                write_hash_size += total_size as u32;
                            }
                            write_hash.insert(key, val);

                            // FLUSHING
                            if write_hash_size > MAX_CHUNK_SIZE_BYTES as u32 {
                                self.flush(&write_hash, &target_page);
                                write_hash_size = 0;
                                write_hash = HashMap::new();
                                target_page -= 1;

                            }
                        }
                        if i == min {
                            break;
                        }
                        i -= 1;
                    }
                    self.flush(&write_hash, &target_page);
                    target_page -= 1;

                    // FLUSH
                    for page in min..=target_page {
                        let delete_page_path = get_full_path(&self.file_root, &PREFIX_FILE_NAME.to_string(), &page);
                        match fs::remove_file(delete_page_path) {
                            Ok(val) => val,
                            Err(_error) => {
                                continue;
                            }
                        }
                    }
                    // remove soorkie_min to soorkie_(num_writes - 1)
                    // remove temps
                }
                thread::sleep(duration);
            }
        }

        fn flush(&mut self, write_hash : &HashMap<String, String>, target_page : &u32) -> Result<(), ErrorKind> {
            println!("Flushing to {target_page}");
            let file_name = get_full_path(&self.file_root, &PREFIX_TEMP_NAME.to_string(), &target_page);
            let target_name = get_full_path(&self.file_root, &PREFIX_FILE_NAME.to_string(), &target_page);
            let mut file = match OpenOptions::new().create(true).write(true).open(&file_name) {
                Ok(file_obj) => file_obj,
                Err(error) => {
                    return Err(ErrorKind::FileReadFailed)
                }
            };
    
            // flush to soorkie_temp_target_page
            let mut offset = 0;
            let mut updates : HashMap<String, DiskPointer> = HashMap::new();
            for (key, val) in write_hash {
                println!("STUFD {key} {val} {offset}");
                let data = encode(key.clone().to_string(), val.to_string())?;
                offset += data.len();
                file.write_all(&data);
                updates.insert(key.to_string(), DiskPointer {
                    file: *target_page,
                    temp: true,
                    offset: offset,
                });
            }
    
            // update keys to soorkie_temp_num_writes
            // - Lock hashmap
            let mut hashmap_lock = self.hash_map.lock().unwrap();
    
            // - Updates keys
            for (key, val) in &updates {
                hashmap_lock.insert(key.to_string(), val.clone());
            }

            // - Unlock hashmap
            drop(hashmap_lock);

            // copy contents of temp to actual
            println!("{}->{}", file_name.display(), &target_name.display());
            match fs::copy(&file_name, target_name) {
                Ok(val) => val,
                Err(_error) => {
                    return Err(ErrorKind::FileCopyFailed)
                }
            };

            match fs::remove_file(file_name) {
                Ok(val) => val,
                Err(_error) => {
                    return Err(ErrorKind::FileDeleteFailed)
                }
            };

            // updates key to actual
            // - Lock hashmaps
            let mut hashmap_lock = self.hash_map.lock().unwrap();
            for (key, val) in updates {
                let mut new_val = val.clone();
                new_val.temp = false;
                hashmap_lock.insert(key, new_val);
            }
            drop(hashmap_lock);
            Ok(())
        }


        pub fn get(&mut self, key: String) -> Option<String> {
            // Check if in hashmap, then return the value
            let mut locked_hash_map = self.hash_map.lock().unwrap();

            if let Some(value) = locked_hash_map.get(&key) {
                let prefix_value = if !value.temp {PREFIX_FILE_NAME.to_string()} else {PREFIX_TEMP_NAME.to_string()};
                println!("GOT in {prefix_value}");
                let real_value = match
                    get_value(&self.file_root, &PREFIX_FILE_NAME.to_string(), &value)
                {
                    Ok(val) => Some(val),
                    Err(_error) => None,
                };
                return real_value;
            }
            let (mut min_bound, _) = get_workspace_bounds(&self.file_root, &PREFIX_FILE_NAME.to_string());
            // Else check all prev maps one by one
            let mut current_file = self.working_file.clone() - 1;
            loop {
                // get search the for current for key
                if
                    let Some(answer) = search_maps(
                        &key,
                        &self.file_root,
                        &PREFIX_FILE_NAME.to_string(),
                        &current_file
                    )
                {
                    return Some(answer);
                }

                if current_file == 0 || current_file == min_bound {
                    break;
                }
                current_file -= 1;
            }

            // Else return key not found
            None
        }
        

        pub fn insert(&mut self, key: String, value: String) -> Result<(), ErrorKind> {
            // check if chunk size is bigger
            if self.working_offset > (MAX_CHUNK_SIZE_BYTES as usize) {
                // Try merge
                println!("NEW MAP");
                self.working_file += 1;
                self.working_offset = 0;
            }

            // encode as bytes using length prefix
            let data: Vec<u8> = encode(key.clone(), value)?;

            let full_path = get_full_path(
                &self.file_root,
                &PREFIX_FILE_NAME.to_string(),
                &self.working_file
            );

            match append_to_file(&full_path, &data) {
                Ok(something) => println!("OK"),
                Err(error) => {
                    println!("Error");
                }
            }

            let target_location = DiskPointer {
                file: self.working_file.clone(),
                temp: false,
                offset: self.working_offset.clone(),
            };

            let mut locked_hash_map = self.hash_map.lock().unwrap();
            locked_hash_map.insert(key.clone(), target_location);
            self.working_offset += data.len();
            Ok(())
        }
    }



    fn append_to_file(file_name: &PathBuf, data: &Vec<u8>) -> Result<(), ErrorKind> {
        let mut file = match OpenOptions::new().create(true).append(true).open(file_name) {
            Ok(file_obj) => file_obj,
            Err(error) => {
                return Err(ErrorKind::FileReadFailed);
            }
        };

        file.write_all(data);
        Ok(())
    }

    fn decode(encoding: Vec<u8>) -> Result<(String, String), ErrorKind> {
        let value: String;

        let total_length = encoding.len();
        if total_length <= 4 {
            return Err(ErrorKind::DataByteLengthSmall);
        }

        let total = ((encoding[0] as u16) << 8) | (encoding[1] as u16);

        if usize::from(total) != total_length {
            return Err(ErrorKind::DataByteCorrupted);
        }

        let key_end = usize::from(((encoding[2] as u16) << 8) | (encoding[3] as u16));
        let key_string = String::from_utf8(encoding[4..key_end].to_vec());
        let val_string = String::from_utf8(encoding[key_end..].to_vec());
        let key = match key_string {
            Ok(key_as_string) => { key_as_string }
            Err(_error) => {
                return Err(ErrorKind::DataByteKeyDecodeFailed);
            }
        };
        let value = match val_string {
            Ok(value_as_string) => { value_as_string }
            Err(_error) => {
                return Err(ErrorKind::DataByteValDecodeFailed);
            }
        };

        Ok((key, value))
    }

    fn encode(key: String, val: String) -> Result<Vec<u8>, ErrorKind> {
        let key_in_bytes: Vec<u8> = key.into_bytes();
        let val_in_bytes: Vec<u8> = val.into_bytes();
        let key_len = key_in_bytes.len();
        let val_len = val_in_bytes.len();

        if val_len + key_len + 4 > u16::MAX.into() {
            return Err(ErrorKind::EncodeDataTooBig);
        }
        // Format: Total 2 bytes, key_end 2 bytes, KEY, VAL
        let total: u16 = (key_len + val_len + 4) as u16;
        let key_end: u16 = (4 + key_len) as u16;

        let mut encoding = Vec::new();
        encoding.extend(total.to_be_bytes());
        encoding.extend(key_end.to_be_bytes());
        encoding.extend(key_in_bytes);
        encoding.extend(val_in_bytes);
        Ok(encoding)
    }

    fn get_full_path(root_path: &String, prefix: &String, idx: &u32) -> PathBuf {
        let file_name = prefix.clone() + "_" + &idx.to_string();
        let full_path = Path::new(root_path).join(file_name);
        full_path
    }

    fn get_workspace_bounds(root_path: &String, prefix: &String) -> (u32, u32) {
        let mut max : u32 = 0;
        let mut min : u32 = u32::MAX;

        let glob_file_name = prefix.clone() + "_*";
        let glob_full_path = Path::new(&root_path)
            .join(glob_file_name)
            .into_os_string()
            .into_string()
            .unwrap();

        let delimiter = prefix.clone() + "_";
        for entry in glob(glob_full_path.as_str()).unwrap() {
            if let Ok(glob_path) = entry {
                let glob_path_str = glob_path.into_os_string().into_string().unwrap();
                let substrings : Vec<&str> = glob_path_str.split(&delimiter).collect();
                let number = match u32::from_str(substrings[1]) {
                    Ok(val) => val, 
                    Err(_error) => {
                        continue;
                    }
                };
                if number + 1 > max {
                    max = number + 1;
                }
                if number < min {
                    min = number
                }
            }
        }
        (min, max)
    }

    fn get_latest(root_path: String, prefix: String) -> u32 {
        let (_, mut latest) = get_workspace_bounds(&root_path, &prefix); 
        // let mut latest: u32 = 0;

        let glob_file_name = prefix.clone() + "_*";
        let glob_full_path = Path::new(&root_path)
            .join(glob_file_name)
            .into_os_string()
            .into_string()
            .unwrap();

        let delimiter = prefix.clone() + "_";
        for entry in glob(glob_full_path.as_str()).unwrap() {
            if let Ok(glob_path) = entry {
                let glob_path_str = glob_path.into_os_string().into_string().unwrap();
                let substrings : Vec<&str> = glob_path_str.split(&delimiter).collect();
                let number = match u32::from_str(substrings[1]) {
                    Ok(val) => val, 
                    Err(_error) => {
                        continue;
                    }
                };
                if number + 1 > latest {
                    latest = number + 1;
                }
                
            }
        }

        // Check if prev file size length is smaller than chunk size
        if latest > 0 {
            let prev_file_path = get_full_path(&root_path, &prefix, &(&latest - 1));
            let prev_file_size = match fs::metadata(prev_file_path) {
                Ok(meta) => meta.len(),
                Err(error) => {
                    // If error reading meta, create a new file
                    MAX_CHUNK_SIZE_BYTES.clone() + 1
                }
            };
            if prev_file_size < MAX_CHUNK_SIZE_BYTES {
                latest -= 1;
            }
        }

        latest
    }

    fn get_maps(
        root_path: &String,
        prefix: &String,
        idx: &u32
    ) -> Result<(HashMap<String, DiskPointer>, usize), ErrorKind> {
        let mut result_map: HashMap<String, DiskPointer> = HashMap::new();

        let full_path = get_full_path(&root_path, &prefix, idx);

        let file = match fs::File::open(full_path) {
            Ok(res) => res,
            Err(_error) => {
                return Err(ErrorKind::FileReadFailed);
            }
        };
        let mut reader = BufReader::new(file);

        // 4 byte size data holder
        let mut size_buffer = [0u8; 2];
        let mut offset: usize = 0;

        loop {
            match reader.read_exact(&mut size_buffer) {
                Ok(val) => val,
                Err(_error) => {
                    break;
                }
            }

            let total_size = ((size_buffer[0] as u16) << 8) + (size_buffer[1] as u16);
            match reader.seek_relative(-2) {
                Ok(val) => val,
                Err(_error) => {
                    break;
                }
            }

            let mut data_buffer: Vec<u8> = vec![0u8; total_size as usize];
            match reader.read_exact(&mut data_buffer) {
                Ok(val) => val,
                Err(_error) => {
                    break;
                }
            }

            let key_val = decode(data_buffer)?;

            let disk_pointer = DiskPointer {
                file: idx.clone(),
                temp: false,
                offset: offset.clone() as usize,
            };

            result_map.insert(key_val.0, disk_pointer);
            offset += total_size as usize;
        }
        Ok((result_map, offset))
    }

    fn search_maps(key: &String, root_path: &String, prefix: &String, idx: &u32) -> Option<String> {
        let full_path = get_full_path(&root_path, &prefix, idx);
        let file = match fs::File::open(full_path) {
            Ok(res) => res,
            Err(_error) => {
                return None;
            }
        };
        let mut reader = BufReader::new(file);

        // 4 byte size data holder
        let mut size_buffer = [0u8; 2];

        let mut result: Option<String> = None;

        loop {
            match reader.read_exact(&mut size_buffer) {
                Ok(val) => val,
                Err(_error) => {
                    break;
                }
            }

            let total_size = ((size_buffer[0] as u16) << 8) + (size_buffer[1] as u16);
            match reader.seek_relative(-2) {
                Ok(val) => val,
                Err(_error) => {
                    break;
                }
            }

            let mut data_buffer: Vec<u8> = vec![0u8; total_size as usize];
            match reader.read_exact(&mut data_buffer) {
                Ok(val) => val,
                Err(_error) => {
                    break;
                }
            }

            let key_val = match decode(data_buffer) {
                Ok(val) => val,
                Err(_error) => {
                    break;
                }
            };

            if key_val.0 == *key {
                result = Some(key_val.1.clone());
            }
        }
        result
    }

    fn get_value(
        root_path: &String,
        prefix: &String,
        pointer: &DiskPointer
    ) -> Result<String, ErrorKind> {
        let full_path = get_full_path(root_path, prefix, &pointer.file);

        let file = match fs::File::open(full_path) {
            Ok(res) => res,
            Err(_error) => {
                return Err(ErrorKind::FileReadFailed);
            }
        };

        let mut reader = BufReader::new(file);
        match reader.seek_relative(pointer.offset as i64) {
            Ok(res) => res,
            Err(_error) => {
                return Err(ErrorKind::FileSeekFailed);
            }
        }

        let mut size_buffer = [0u8; 2];
        match reader.read_exact(&mut size_buffer) {
            Ok(val) => val,
            Err(_error) => {
                return Err(ErrorKind::DataByteLenDecodeFailed);
            }
        }

        let total_size = ((size_buffer[0] as u16) << 8) + (size_buffer[1] as u16);
        match reader.seek_relative(-2) {
            Ok(val) => val,
            Err(_error) => {
                return Err(ErrorKind::FileSeekFailed);
            }
        }

        let mut data_buffer: Vec<u8> = vec![0u8; total_size as usize];
        match reader.read_exact(&mut data_buffer) {
            Ok(val) => val,
            Err(_error) => {
                return Err(ErrorKind::DataByteValDecodeFailed);
            }
        }

        let key_val = decode(data_buffer)?;
        Ok(key_val.1)
    }

    fn print_maps(root_path: &String, prefix: &String, map: &HashMap<String, DiskPointer>) {
        for (key, val) in map {
            let value = match get_value(root_path, prefix, &val) {
                Ok(val) => val,
                Err(_error) => {
                    println!("Unable to get value of {key}");
                    continue;
                }
            };
            println!("{key}: {value}");
        }
    }
}
