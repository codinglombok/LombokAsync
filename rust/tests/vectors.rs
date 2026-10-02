//! Runs the shared cross-language vectors (vectors/lombokasync-vectors-v1.json).

use std::future::{pending, Future};
use std::pin::Pin;
use std::time::Duration;

use lombokasync::{block_on, mpsc, oneshot, select_all, timeout, try_join_all, yield_now};
use serde_json::{json, Value};

type Task = Pin<Box<dyn Future<Output = Result<Value, String>>>>;

fn task(spec: &Value) -> Task {
    let yields = spec.get("yields").and_then(Value::as_u64).unwrap_or(0);
    if spec.get("never").and_then(Value::as_bool) == Some(true) {
        return Box::pin(pending());
    }
    let outcome = match spec.get("error") {
        Some(e) => Err(e.as_str().unwrap().to_string()),
        None => Ok(spec["value"].clone()),
    };
    Box::pin(async move {
        for _ in 0..yields {
            yield_now().await;
        }
        outcome
    })
}

fn run_mpsc(ops: &[Value]) -> Vec<Value> {
    let mut out = Vec::new();
    let mut senders: Vec<Option<mpsc::Sender<Value>>> = Vec::new();
    let mut rx: Option<mpsc::Receiver<Value>> = None;
    for op in ops {
        let name = op[0].as_str().unwrap();
        let r = match name {
            "new" => {
                // capacity < 1 is invalid; negative values cannot be a usize at all
                let made = match op[1].as_i64() {
                    None => Ok(mpsc::channel()),
                    Some(c) => usize::try_from(c)
                        .map_err(|_| mpsc::InvalidCapacity)
                        .and_then(mpsc::bounded),
                };
                match made {
                    Ok((tx, r)) => {
                        senders.push(Some(tx));
                        rx = Some(r);
                        json!("ok")
                    }
                    Err(_) => {
                        out.push(json!("invalid_capacity"));
                        break;
                    }
                }
            }
            "clone" => {
                let s = senders[op[1].as_u64().unwrap() as usize]
                    .as_ref()
                    .unwrap()
                    .clone();
                senders.push(Some(s));
                json!({ "sender": senders.len() - 1 })
            }
            "send" => match senders[op[1].as_u64().unwrap() as usize]
                .as_ref()
                .unwrap()
                .try_send(op[2].clone())
            {
                Ok(()) => json!("ok"),
                Err(mpsc::TrySendError::Full(_)) => json!("full"),
                Err(mpsc::TrySendError::Closed(_)) => json!("closed"),
            },
            "recv" => match rx.as_mut().unwrap().try_recv() {
                Ok(v) => json!({ "value": v }),
                Err(mpsc::TryRecvError::Empty) => json!("empty"),
                Err(mpsc::TryRecvError::Closed) => json!("closed"),
            },
            "drop" => {
                senders[op[1].as_u64().unwrap() as usize] = None;
                json!("ok")
            }
            "close" => {
                rx.as_mut().unwrap().close();
                json!("ok")
            }
            "len" => json!({ "len": rx.as_ref().unwrap().len() }),
            other => panic!("unknown op {other}"),
        };
        out.push(r);
    }
    out
}

fn run_oneshot(ops: &[Value]) -> Vec<Value> {
    let (tx, mut rx) = oneshot::channel::<Value>();
    // Rust's send consumes the sender, so a second send is reported as already_sent.
    let mut tx = Some(tx);
    let mut used = false;
    let mut out = Vec::new();
    for op in ops {
        let r = match op[0].as_str().unwrap() {
            "new" => json!("ok"),
            "send" => match tx.take() {
                None if used => json!("already_sent"),
                None => panic!("vectors never send after drop_tx"),
                Some(s) => {
                    used = true;
                    match s.send(op[1].clone()) {
                        Ok(()) => json!("ok"),
                        Err(_) => json!("closed"),
                    }
                }
            },
            "recv" => match rx.try_recv() {
                Ok(v) => json!({ "value": v }),
                Err(oneshot::TryRecvError::Empty) => json!("empty"),
                Err(oneshot::TryRecvError::Closed) => json!("closed"),
            },
            "drop_tx" => {
                tx = None;
                json!("ok")
            }
            "close" => {
                rx.close();
                json!("ok")
            }
            other => panic!("unknown op {other}"),
        };
        out.push(r);
    }
    out
}

#[test]
fn vectors() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../vectors/lombokasync-vectors-v1.json"
    );
    let doc: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let deadline = Duration::from_millis(doc["timeout_deadline_ms"].as_u64().unwrap());
    let cases = doc["cases"].as_array().unwrap();
    assert!(cases.len() >= 100);
    let mut failures = Vec::new();
    for case in cases {
        let got = match case["kind"].as_str().unwrap() {
            "mpsc" => Value::Array(run_mpsc(case["ops"].as_array().unwrap())),
            "oneshot" => Value::Array(run_oneshot(case["ops"].as_array().unwrap())),
            "join_all" => {
                let tasks = case["tasks"].as_array().unwrap().iter().map(task).collect();
                match block_on(try_join_all(tasks)) {
                    Ok(v) => json!({ "ok": v }),
                    Err(e) => json!({ "error": e }),
                }
            }
            "select" => {
                let tasks = case["tasks"].as_array().unwrap().iter().map(task).collect();
                match block_on(select_all(tasks)) {
                    (i, Ok(v)) => json!({ "index": i, "value": v }),
                    (_, Err(e)) => json!({ "error": e }),
                }
            }
            "timeout" => match block_on(timeout(deadline, task(&case["task"]))) {
                Ok(Ok(v)) => json!({ "ok": v }),
                Ok(Err(e)) => json!({ "error": e }),
                Err(_) => json!("timeout"),
            },
            other => panic!("unknown kind {other}"),
        };
        if got != case["expected"] {
            failures.push(format!(
                "{}: got {} expected {}",
                case["id"], got, case["expected"]
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} vector failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
