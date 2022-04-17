use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::{Client, Error};
use aws_sdk_dynamodb::model::AttributeValue;
use aws_sdk_dynamodb::types::Blob;
use bytes::{BufMut, BytesMut};
use dyn_ts::{Chunk, Column, ColumnType, Item};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let region_provider = RegionProviderChain::default_provider().or_else("ap-southeast-1");
    let config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&config);

    let columns = vec![
        Column {
            name: "city_name".to_string(),
            r#type: ColumnType::Text,
        },
        Column {
            name: "temp_c".to_string(),
            r#type: ColumnType::Float32,
        },
        Column {
            name: "wind_speed_ms".to_string(),
            r#type: ColumnType::Float32,
        },
    ];
    let dimensions = vec![
        Column {
            name: "city_name".to_string(),
            r#type: ColumnType::Text,
        },
    ];
    let data = vec![
        vec![Item::Text("HO CHI MINH".to_string()), Item::Float32(27.5), Item::Float32(6.7)],
        vec![Item::Text("HANOI".to_string()), Item::Float32(17.5), Item::Float32(7.7)],
    ];
    let mut data_in_bytes = BytesMut::new();
    let mut sizes = BytesMut::new();
    for row in data.into_iter() {
        for item in row {
            match item {
                Item::Text(s) => {
                    data_in_bytes.put_slice(s.as_bytes());
                    sizes.put_u8(s.as_bytes().len() as u8)
                }
                Item::U32(v) => {
                    data_in_bytes.put_u32(v);
                    sizes.put_u8(4)
                }
                Item::Float32(v) => {
                    data_in_bytes.put_f32(v);
                    sizes.put_u8(4)
                }
            }
        }
    }
    let data = data_in_bytes.to_vec();
    let sizes = sizes.to_vec();
    // let chunk = Chunk::new("1".to_string(), "01012022".to_string(), sizes, dimensions, columns, data);
    // println!("{:?}", chunk.data());
    // println!("{:?}", chunk.size());
    // println!("{:?}", chunk.secondary_index());

    // client.put_item()
    //     .table_name("timeseries")
    //     .item("id", AttributeValue::S("1".to_string()))
    //     .item("timestamp", AttributeValue::S(chunk.secondary_index()))
    //     .item("sizes", AttributeValue::B(Blob::new(chunk.sizes)))
    //     .item("data", AttributeValue::B(Blob::new(chunk.data)))
    //     .send()
    //     .await
    //     .unwrap();
    let req = client
        .get_item()
        .table_name("timeseries")
        .key(
            "id",
            AttributeValue::S("1".to_string())
        )
        .key(
            "timestamp",
            AttributeValue::S("01012022__city_name".to_string())
        )
        .send()
        .await
        .unwrap();

    println!("{:?}", req.item);
    let item = req.item.unwrap();
    let id = item.get("id").unwrap().as_s().unwrap().clone();
    let timestamp = item.get("timestamp").unwrap().as_s().unwrap().clone();
    let sizes = item.get("sizes").unwrap().as_b().unwrap().clone().as_ref().to_vec();
    let data = item.get("data").unwrap().as_b().unwrap().clone().as_ref().to_vec();
    let chunk = Chunk::new(id, timestamp, sizes, dimensions, columns, data);
    println!("{:?}", chunk.data());
    println!("{:?}", chunk.size());
    println!("{:?}", chunk.secondary_index());

    Ok(())
}