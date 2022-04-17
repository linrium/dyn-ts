use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt};

const LIMIT_ITEM_SIZE: u32 = 400_000;

pub enum ColumnType {
    U32,
    Float32,
    Text,
}

pub struct Column {
    pub name: String,
    pub r#type: ColumnType,
}

impl Column {
    pub fn read(&self, bytes: &[u8]) -> Item {
        match self.r#type {
            ColumnType::U32 => {
                let mut rdr = Cursor::new(bytes.to_vec());
                let n = rdr.read_u32::<BigEndian>().unwrap();
                Item::U32(n)
            }
            ColumnType::Float32 => {
                let mut rdr = Cursor::new(bytes.to_vec());
                let n = rdr.read_f32::<BigEndian>().unwrap();
                Item::Float32(n)
            }
            ColumnType::Text => {
                let mut s = String::new();
                for i in 0..bytes.len() {
                    s.push_str(&format!("{}", bytes[i] as char));
                }
                Item::Text(s)
            }
        }
    }
}

/*
| id | timestamp | sizes | data |

| 1 | 01/01/2022 | 4,4,4 |HCM, 29.5, 6.7 |

size: size_of(id) + size_of(timestamp) + size_of(data)
size_of(data): foreach sizeof(header_types)
 */
pub struct Chunk {
    pub id: String,
    pub timestamp: String,
    pub sizes: Vec<u8>,
    pub dimensions: Vec<Column>,
    pub columns: Vec<Column>,
    pub data: Vec<u8>,
}

impl Chunk {
    pub fn new(id: String, timestamp: String, sizes: Vec<u8>, dimensions: Vec<Column>, columns: Vec<Column>, data: Vec<u8>) -> Self {
        Self {
            id,
            timestamp,
            sizes,
            dimensions,
            columns,
            data,
        }
    }

    pub fn secondary_index(&self) -> String {
        let d = self.dimensions.iter().map(|c| c.name.clone()).collect::<Vec<_>>().join("_");
        format!("{}__{}", self.timestamp, d)
    }

    pub fn size(&self) -> u32 {
        (self.data.len() + self.sizes.len() + 9) as u32
    }

    pub fn write(&self, items: Vec<Vec<Item>>) {
        while self.size() <= LIMIT_ITEM_SIZE {

        }

        // Trigger create new row
    }

    pub fn data(&self) -> Vec<Vec<Item>> {
        let mut current = 0;
        let mut index = 0;
        let mut result = vec![];
        let mut rows = vec![];
        for (i, pos) in self.sizes.iter().enumerate() {
            let pos = pos.clone() as usize;
            let bytes = &self.data[current..current + pos];
            let item = self.columns[i % self.columns.len()].read(bytes);
            rows.push(item);

            current += pos;
            index += 1;
            if index == self.columns.len() {
                index = 0;
                result.push(rows.clone());
                rows.clear();
            }
        }

        result
    }
}

#[derive(Debug, Clone)]
pub enum Item {
    U32(u32),
    Float32(f32),
    Text(String),
}
/*
| id | interval | dimensions | headers | header_types |
| 1 | 15 days | city_name, weather_type_id | city_name, temp_c, wind_speed_ms | 4, 3, 3 |
 */
pub struct Hypertable {
    id: String,
    columns: Vec<Column>,
    sizes: Vec<u32>,
    tables: Vec<Chunk>,
}

impl Hypertable {
    pub fn new(id: String, columns: Vec<Column>) -> Self {
        Self {
            id,
            columns,
            sizes: Vec::new(),
            tables: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use super::*;

}