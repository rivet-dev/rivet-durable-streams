use super::*;

pub(super) fn parse_producer(request: &Request) -> Result<Option<ProducerHeaders>, String> {
    let id = public_header_bytes(request, PRODUCER_ID).map_err(|error| error.to_string())?;
    let epoch = public_header_text(request, PRODUCER_EPOCH).map_err(|error| error.to_string())?;
    let seq = public_header_text(request, PRODUCER_SEQ).map_err(|error| error.to_string())?;
    let any = id.is_some() || epoch.is_some() || seq.is_some();
    let all = id.is_some() && epoch.is_some() && seq.is_some();
    if any && !all {
        return Err(
			"All producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be provided together"
				.to_owned(),
		);
    }
    if !all {
        return Ok(None);
    }
    let id = id.expect("producer id checked above");
    if id.is_empty() {
        return Err("Invalid Producer-Id: must not be empty".to_owned());
    }
    let epoch = parse_non_negative_safe_integer(
        epoch.as_deref().expect("producer epoch checked above"),
        "Producer-Epoch",
    )
    .map_err(|error| error.to_string())?;
    let seq = parse_non_negative_safe_integer(
        seq.as_deref().expect("producer seq checked above"),
        "Producer-Seq",
    )
    .map_err(|error| error.to_string())?;
    Ok(Some(ProducerHeaders {
        id: base64_url(&id),
        epoch,
        seq,
    }))
}

pub(super) fn validate_producer(
    state: Option<&ProducerState>,
    producer: &ProducerHeaders,
    now: i64,
) -> ProducerValidation {
    let Some(state) = state else {
        return if producer.seq == 0 {
            ProducerValidation::Accepted(ProducerState {
                epoch: producer.epoch,
                last_seq: 0,
                last_updated: now,
            })
        } else {
            ProducerValidation::SequenceGap {
                expected: 0,
                received: producer.seq,
            }
        };
    };
    if producer.epoch < state.epoch {
        return ProducerValidation::StaleEpoch {
            current_epoch: state.epoch,
        };
    }
    if producer.epoch > state.epoch {
        return if producer.seq == 0 {
            ProducerValidation::Accepted(ProducerState {
                epoch: producer.epoch,
                last_seq: 0,
                last_updated: now,
            })
        } else {
            ProducerValidation::InvalidEpochSeq
        };
    }
    if producer.seq <= state.last_seq {
        return ProducerValidation::Duplicate {
            last_seq: state.last_seq,
        };
    }
    if producer.seq == state.last_seq + 1 {
        return ProducerValidation::Accepted(ProducerState {
            epoch: producer.epoch,
            last_seq: producer.seq,
            last_updated: now,
        });
    }
    ProducerValidation::SequenceGap {
        expected: state.last_seq + 1,
        received: producer.seq,
    }
}

pub(super) fn producer_failure(
    failure: ProducerValidation,
    producer: &ProducerHeaders,
    meta: &Meta,
    close_only: bool,
) -> Result<Response> {
    match failure {
        ProducerValidation::Duplicate { last_seq } => {
            let mut headers = vec![
                (PRODUCER_EPOCH.to_owned(), producer.epoch.to_string()),
                (PRODUCER_SEQ.to_owned(), last_seq.to_string()),
            ];
            if close_only {
                headers.push((STREAM_OFFSET.to_owned(), meta.current_offset.clone()));
                if meta.closed {
                    headers.push((STREAM_CLOSED.to_owned(), "true".to_owned()));
                }
            }
            response(204, headers, Vec::new())
        }
        ProducerValidation::StaleEpoch { current_epoch } => text_response(
            403,
            "Stale producer epoch",
            [(PRODUCER_EPOCH, current_epoch.to_string())],
        ),
        ProducerValidation::InvalidEpochSeq => text_response(
            400,
            "New epoch must start with sequence 0",
            [] as [(&str, &str); 0],
        ),
        ProducerValidation::SequenceGap { expected, received } => text_response(
            409,
            "Producer sequence gap",
            [
                (PRODUCER_EXPECTED_SEQ, expected.to_string()),
                (PRODUCER_RECEIVED_SEQ, received.to_string()),
            ],
        ),
        ProducerValidation::Accepted(_) => Err(anyhow!("accepted producer used as failure")),
    }
}

pub(super) fn producer_success(
    offset: &str,
    producer: Option<&ProducerHeaders>,
    closed: bool,
) -> Result<Response> {
    let mut headers = vec![(STREAM_OFFSET.to_owned(), offset.to_owned())];
    if let Some(producer) = producer {
        headers.push((PRODUCER_EPOCH.to_owned(), producer.epoch.to_string()));
        headers.push((PRODUCER_SEQ.to_owned(), producer.seq.to_string()));
    }
    if closed {
        headers.push((STREAM_CLOSED.to_owned(), "true".to_owned()));
    }
    response(
        if producer.is_some() { 200 } else { 204 },
        headers,
        Vec::new(),
    )
}

pub(super) fn close_success(offset: &str, producer: Option<&ProducerHeaders>) -> Result<Response> {
    let mut headers = vec![
        (STREAM_OFFSET.to_owned(), offset.to_owned()),
        (STREAM_CLOSED.to_owned(), "true".to_owned()),
    ];
    if let Some(producer) = producer {
        headers.push((PRODUCER_EPOCH.to_owned(), producer.epoch.to_string()));
        headers.push((PRODUCER_SEQ.to_owned(), producer.seq.to_string()));
    }
    response(204, headers, Vec::new())
}

pub(super) fn closed_by_matches(meta: &Meta, producer: &ProducerHeaders) -> bool {
    meta.closed_by
        .as_deref()
        .and_then(|value| serde_json::from_str::<ClosedBy>(value).ok())
        .is_some_and(|closed_by| {
            closed_by.producer_id == producer.id
                && closed_by.epoch == producer.epoch
                && closed_by.seq == producer.seq
        })
}
