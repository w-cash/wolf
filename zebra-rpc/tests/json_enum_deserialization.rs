//! Public response unions must deserialize the same numeric fields as their
//! concrete objects, including when another workspace crate enables
//! `serde_json/arbitrary_precision`.

use std::collections::HashMap;

use zebra_rpc::client::{
    BlockHeaderObject, BlockObject, GetBlockHeaderResponse, GetBlockResponse, GetBlockTransaction,
    GetRawMempoolResponse, GetRawTransactionResponse, MempoolObject, TransactionObject,
};

const BLOCK: &str = include_str!("vectors/getblock_response_1.json");
const BLOCK_WITH_TRANSACTIONS: &str = include_str!("vectors/getblock_response_2.json");
const TRANSACTION: &str = include_str!("vectors/getrawtransaction_response_true.json");
const LARGE_INTEGER: i64 = 9_007_199_254_740_993;
const MEMPOOL: &str = r#"{
    "transaction": {
        "size": 18446744073709551615,
        "fee": 0.00010000,
        "modifiedfee": 2e-4,
        "time": -9007199254740993,
        "height": 2931716,
        "descendantcount": 9007199254740993,
        "descendantsize": 9007199254740993,
        "descendantfees": 9007199254740993,
        "depends": []
    }
}"#;

/// Replace one fixture number without reparsing or normalizing any JSON token.
fn replace_number(json: &str, field: &str, replacement: &str) -> String {
    let key = format!("\"{field}\":");
    let after_key = json.find(&key).expect("fixture contains the field") + key.len();
    let start = after_key + json[after_key..].len() - json[after_key..].trim_start().len();
    let len = json[start..]
        .bytes()
        .take_while(|byte| byte.is_ascii_digit() || b"-.+eE".contains(byte))
        .count();
    assert!(len > 0, "fixture field is a number");
    format!("{}{}{}", &json[..start], replacement, &json[start + len..])
}

fn append_field(json: &str, field: &str) -> String {
    let prefix = json
        .trim_end()
        .strip_suffix('}')
        .expect("fixture is an object");
    format!("{prefix},{field}}}")
}

#[test]
fn verbose_unions_match_concrete_objects() -> Result<(), Box<dyn std::error::Error>> {
    for json in [BLOCK, BLOCK_WITH_TRANSACTIONS] {
        let concrete: BlockObject = serde_json::from_str(json)?;
        let response: GetBlockResponse = serde_json::from_str(json)?;
        assert_eq!(response, GetBlockResponse::Object(Box::new(concrete)));
    }

    let header: BlockHeaderObject = serde_json::from_str(BLOCK)?;
    assert_eq!(
        serde_json::from_str::<GetBlockHeaderResponse>(BLOCK)?,
        GetBlockHeaderResponse::Object(Box::new(header))
    );

    let transaction: TransactionObject = serde_json::from_str(TRANSACTION)?;
    assert_eq!(
        serde_json::from_str::<GetRawTransactionResponse>(TRANSACTION)?,
        GetRawTransactionResponse::Object(Box::new(transaction.clone()))
    );
    assert_eq!(
        serde_json::from_str::<GetBlockTransaction>(TRANSACTION)?,
        GetBlockTransaction::Object(Box::new(transaction))
    );
    Ok(())
}

#[test]
fn mempool_decimals_and_integer_fields_keep_their_types() -> Result<(), Box<dyn std::error::Error>>
{
    let concrete: HashMap<String, MempoolObject> = serde_json::from_str(MEMPOOL)?;
    let response: GetRawMempoolResponse = serde_json::from_str(MEMPOOL)?;
    assert_eq!(response, GetRawMempoolResponse::Verbose(concrete.clone()));

    let object = concrete
        .get("transaction")
        .ok_or("fixture entry is missing")?;
    let fee_atoms: i64 = object.fee().into();
    let modified_atoms: i64 = object.modified_fee().into();
    assert_eq!(fee_atoms, 10_000);
    assert_eq!(modified_atoms, 20_000);
    assert_eq!(object.size(), u64::MAX);
    assert_eq!(object.time(), -LARGE_INTEGER);
    assert_eq!(object.descendantfees(), 9_007_199_254_740_993);
    assert!(serde_json::to_string(&response)?.contains("\"descendantfees\":9007199254740993"));
    Ok(())
}

#[test]
fn nested_atomic_amounts_and_block_counters_are_not_rounded(
) -> Result<(), Box<dyn std::error::Error>> {
    let transaction = replace_number(TRANSACTION, "valueZat", "9007199254740993");
    for nested in [false, true] {
        let object = if nested {
            let GetBlockTransaction::Object(object) = serde_json::from_str(&transaction)? else {
                return Err("verbose fixture is not an object".into());
            };
            object
        } else {
            let GetRawTransactionResponse::Object(object) = serde_json::from_str(&transaction)?
            else {
                return Err("verbose fixture is not an object".into());
            };
            object
        };
        assert_eq!(object.outputs()[0].value_zat(), LARGE_INTEGER);
        assert!(serde_json::to_string(&object)?.contains("\"valueZat\":9007199254740993"));
    }

    let block = replace_number(BLOCK, "confirmations", "9007199254740993");
    let block = replace_number(&block, "time", "-9007199254740993");
    let GetBlockResponse::Object(object) = serde_json::from_str(&block)? else {
        return Err("verbose fixture is not an object".into());
    };
    assert_eq!(object.confirmations(), LARGE_INTEGER);
    assert_eq!(object.time(), Some(-LARGE_INTEGER));
    Ok(())
}

#[test]
fn decimal_spellings_match_direct_typed_deserialization() -> Result<(), Box<dyn std::error::Error>>
{
    for decimal in [
        "7.965079500685483e7",
        "79650795.006854830000",
        "1.234567890123456789012345678901",
    ] {
        let json = replace_number(BLOCK, "difficulty", decimal);
        let block: BlockObject = serde_json::from_str(&json)?;
        assert_eq!(
            serde_json::from_str::<GetBlockResponse>(&json)?,
            GetBlockResponse::Object(Box::new(block))
        );
        let header: BlockHeaderObject = serde_json::from_str(&json)?;
        assert_eq!(
            serde_json::from_str::<GetBlockHeaderResponse>(&json)?,
            GetBlockHeaderResponse::Object(Box::new(header))
        );
    }
    Ok(())
}

#[test]
fn raw_and_empty_variants_keep_their_wire_shapes() -> Result<(), Box<dyn std::error::Error>> {
    let raw = r#""00abcd""#;
    let block: GetBlockResponse = serde_json::from_str(raw)?;
    let header: GetBlockHeaderResponse = serde_json::from_str(raw)?;
    let transaction: GetRawTransactionResponse = serde_json::from_str(raw)?;
    assert!(matches!(&block, GetBlockResponse::Raw(_)));
    assert!(matches!(&header, GetBlockHeaderResponse::Raw(_)));
    assert!(matches!(&transaction, GetRawTransactionResponse::Raw(_)));
    assert_eq!(serde_json::to_string(&block)?, raw);
    assert_eq!(serde_json::to_string(&header)?, raw);
    assert_eq!(serde_json::to_string(&transaction)?, raw);

    let hash = format!("\"{}\"", "00".repeat(32));
    let transaction: GetBlockTransaction = serde_json::from_str(&hash)?;
    assert!(matches!(&transaction, GetBlockTransaction::Hash(_)));
    assert_eq!(serde_json::to_string(&transaction)?, hash);

    for wire in ["[]", "{}", r#"["transaction"]"#] {
        let response: GetRawMempoolResponse = serde_json::from_str(wire)?;
        assert_eq!(serde_json::to_string(&response)?, wire);
    }
    Ok(())
}

#[test]
fn invalid_shapes_hex_and_required_fields_remain_rejected() {
    for wire in [
        "null", "false", "7", "1.2345", "[]", "{}", r#""zz""#, r#""0""#,
    ] {
        assert!(serde_json::from_str::<GetBlockResponse>(wire).is_err());
        assert!(serde_json::from_str::<GetBlockHeaderResponse>(wire).is_err());
        assert!(serde_json::from_str::<GetRawTransactionResponse>(wire).is_err());
        assert!(serde_json::from_str::<GetBlockTransaction>(wire).is_err());
    }
    for wire in [
        "null",
        "false",
        "7",
        "1.2345",
        r#""transaction""#,
        "[7]",
        r#"{"tx":7}"#,
    ] {
        assert!(serde_json::from_str::<GetRawMempoolResponse>(wire).is_err());
    }
    let duplicate = append_field(BLOCK, "\"difficulty\":1.0");
    assert!(serde_json::from_str::<GetBlockResponse>(&duplicate).is_err());
    assert!(serde_json::from_str::<GetBlockHeaderResponse>(&duplicate).is_err());
    let missing = BLOCK.replacen("\"nTx\":", "\"unused_nTx\":", 1);
    assert!(serde_json::from_str::<GetBlockResponse>(&missing).is_err());
    for value in ["9223372036854775808", "0.5", "\"1\"", "null"] {
        let json = replace_number(BLOCK, "confirmations", value);
        assert!(serde_json::from_str::<GetBlockResponse>(&json).is_err());
    }
    for value in ["18446744073709551616", "-1", "0.5", "\"1\""] {
        let json = replace_number(MEMPOOL, "size", value);
        assert!(serde_json::from_str::<GetRawMempoolResponse>(&json).is_err());
    }
}

#[test]
fn unknown_fields_and_reader_entry_point_remain_compatible(
) -> Result<(), Box<dyn std::error::Error>> {
    let extended = append_field(BLOCK, "\"future_field\":{\"decimal\":1.234567890123456789}");
    assert_eq!(
        serde_json::from_str::<GetBlockResponse>(&extended)?,
        serde_json::from_str::<GetBlockResponse>(BLOCK)?
    );
    assert_eq!(
        serde_json::from_reader::<_, GetBlockResponse>(BLOCK.as_bytes())?,
        serde_json::from_slice::<GetBlockResponse>(BLOCK.as_bytes())?
    );
    Ok(())
}
