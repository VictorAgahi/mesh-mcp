//! Calls of a helper the project declares as a producer (`[[engines.contracts.producer_calls]]`,
//! 9.0 T14b): the call is read in the syntax tree, every producer is `heuristic`, a name that
//! cannot be read is counted, never invented.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::ProducerCallConfig;
use mesh_parsers::{ExtractConfig, FileIndex, PolyglotIndexer};
use std::path::Path;

fn call(callee: &str, arg: usize, topic_keys: &[&str], queue_keys: &[&str]) -> ProducerCallConfig {
    ProducerCallConfig {
        name: "c".to_string(),
        callee: callee.to_string(),
        arg,
        topic_keys: topic_keys.iter().map(ToString::to_string).collect(),
        queue_keys: queue_keys.iter().map(ToString::to_string).collect(),
    }
}

fn index_with(calls: Vec<ProducerCallConfig>, file: &str, code: &str) -> FileIndex {
    let cfg = ExtractConfig {
        producer_calls: calls,
        ..ExtractConfig::default()
    };
    let fi = PolyglotIndexer::extract_with_config(Path::new(file), code, 0, &cfg);
    assert!(!fi.parse_failed, "{file} must parse");
    fi
}

fn outbox() -> Vec<ProducerCallConfig> {
    vec![call("JobsOutbox.createJob", 0, &["type"], &["target"])]
}

/// `(topic, line)` of every producer, in file order.
fn producers(fi: &FileIndex) -> Vec<(String, usize)> {
    fi.producers
        .iter()
        .map(|(i, t)| (t.to_string(), fi.nodes[*i].line_start))
        .collect()
}

const ENUMS: &str =
    "import { JobType, Queue } from './enums';\nimport { JobsOutbox } from './outbox';\n";

#[test]
fn an_object_call_yields_the_type_and_the_queue_on_their_own_lines() {
    let code = format!(
        "{ENUMS}export const j = () => JobsOutbox.createJob({{\n  type: JobType.SCAN_FILE,\n  \
         payload: {{}},\n  target: Queue.STORAGE,\n}});\n"
    );
    let fi = index_with(outbox(), "svc/jobs.ts", &code);
    assert_eq!(
        producers(&fi),
        vec![
            ("JobType.SCAN_FILE".to_string(), 4),
            ("Queue.STORAGE".to_string(), 6)
        ]
    );
    // The queue resolves by value when the file is folded, the job type keeps its member key.
    let queue: Vec<bool> = fi.member_sites.iter().map(|m| m.queue).collect();
    assert_eq!(queue, vec![false, true]);
    assert!(fi.member_sites.iter().all(|m| m.producer));
}

#[test]
fn the_declared_argument_index_is_the_one_read() {
    let calls = vec![call("EventOutbox.createFollowUp", 1, &["type"], &[])];
    let code = "import { EventOutbox } from './outbox';\n\
                EventOutbox.createFollowUp({ type: 'source.event' }, { type: 'follow.up' });\n\
                EventOutbox.createFollowUp({ type: 'only.source' });\n";
    let fi = index_with(calls, "svc/f.ts", code);
    assert_eq!(producers(&fi), vec![("follow.up".to_string(), 2)]);
}

#[test]
fn every_declared_producer_is_heuristic() {
    let code = format!("{ENUMS}JobsOutbox.createJob({{ type: JobType.A, target: Queue.B }});\n");
    let fi = index_with(outbox(), "svc/jobs.ts", &code);
    assert_eq!(fi.producers.len(), 2);
    let mut marked = fi.heuristic_topic_nodes.clone();
    marked.sort_unstable();
    let mut nodes: Vec<usize> = fi.producers.iter().map(|(i, _)| *i).collect();
    nodes.sort_unstable();
    assert_eq!(marked, nodes);
}

#[test]
fn a_literal_a_file_constant_and_an_array_are_names() {
    let calls = vec![
        call(
            "EventOutbox.createEvent",
            0,
            &["type", "targetServices"],
            &[],
        ),
        call("bus.publish", 0, &[], &[]),
    ];
    let code = "import { Streams } from './streams';\n\
                import { EventOutbox } from './outbox';\n\
                const LOCAL = 'local-topic';\n\
                EventOutbox.createEvent({\n  type: 'user.created',\n  targetServices: [\n    Streams.A,\n    LOCAL,\n  ],\n});\n\
                bus.publish('plain.topic', {});\n";
    let fi = index_with(calls, "svc/e.ts", code);
    assert_eq!(
        producers(&fi),
        vec![
            ("user.created".to_string(), 5),
            ("Streams.A".to_string(), 7),
            ("local-topic".to_string(), 8),
            ("plain.topic".to_string(), 11),
        ]
    );
}

#[test]
fn another_receiver_a_shadowing_parameter_a_comment_and_a_string_are_not_the_helper() {
    let code = format!(
        "{ENUMS}declare const Other: any;\n\
         Other.createJob({{ type: JobType.A, target: Queue.B }});\n\
         function f(JobsOutbox: any) {{ JobsOutbox.createJob({{ type: JobType.C, target: Queue.D }}); }}\n\
         // JobsOutbox.createJob({{ type: JobType.E, target: Queue.F }});\n\
         const s = \"JobsOutbox.createJob({{ type: JobType.G, target: Queue.H }})\";\n\
         const ref = JobsOutbox.createJob;\n"
    );
    let fi = index_with(outbox(), "svc/decoys.ts", &code);
    assert_eq!(producers(&fi), vec![]);
    assert_eq!(fi.declared_unread, 0);
}

#[test]
fn a_receiver_qualifier_matches_on_a_segment_boundary() {
    let code = format!(
        "{ENUMS}this.deps.JobsOutbox.createJob({{ type: JobType.A }});\n\
         MyJobsOutbox.createJob({{ type: JobType.B }});\n\
         getOutbox().createJob({{ type: JobType.C }});\n"
    );
    let fi = index_with(outbox(), "svc/q.ts", &code);
    assert_eq!(producers(&fi), vec![("JobType.A".to_string(), 3)]);
    // `getOutbox()` is no name that can be compared with the declared receiver: counted.
    assert_eq!(fi.declared_unread, 1);
}

#[test]
fn a_receiver_wrapped_in_a_cast_a_non_null_or_parentheses_is_still_read() {
    let calls = vec![
        call("JobsOutbox.createJob", 0, &["type"], &[]),
        call("jobs.enqueue", 0, &["type"], &[]),
    ];
    let code = "import { JobsOutbox } from './outbox';\n\
                (JobsOutbox as any).createJob({ type: 'cast-job' });\n\
                JobsOutbox!.createJob({ type: 'bang-job' });\n\
                (this.deps.JobsOutbox!).createJob({ type: 'deep-job' });\n\
                (JobsOutbox.createJob as any)({ type: 'callee-cast-job' });\n\
                this.jobs!.enqueue({ type: 'field-job' });\n\
                (<any>JobsOutbox).createJob({ type: 'angle-job' });\n\
                other().createJob({ type: 'not-read' });\n";
    let fi = index_with(calls, "svc/wrapped.ts", code);
    let got: Vec<String> = producers(&fi).into_iter().map(|(t, _)| t).collect();
    assert_eq!(
        got,
        vec![
            "cast-job",
            "bang-job",
            "deep-job",
            "callee-cast-job",
            "field-job"
        ]
    );
    // The call-expression receiver and the angle-bracket cast are counted, never guessed.
    assert_eq!(fi.declared_unread, 2);
}

#[test]
fn a_placeholder_default_is_kept_only_when_key_and_default_are_names() {
    // Built at runtime: no key-shaped literal in the source of this file.
    let weak = ["hunter", "2"].concat();
    let weak_prod = ["hunter", "2-prod"].concat();
    let code = format!(
        "import {{ JobsOutbox }} from './outbox';\n\
         JobsOutbox.createJob({{ type: process.env.DB_PASSWORD || '{weak}' }});\n\
         JobsOutbox.createJob({{ type: process.env.TOPIC_X || '{weak_prod}' }});\n\
         JobsOutbox.createJob({{ type: process.env.API_TOKEN ?? 'plain-topic' }});\n\
         JobsOutbox.createJob({{ type: process.env.TOPIC_Y || 'orders' }});\n\
         JobsOutbox.createJob({{ type: process.env.TOPIC_Z }});\n"
    );
    let fi = index_with(outbox(), "svc/ph.ts", &code);
    assert_eq!(
        producers(&fi),
        vec![
            ("${TOPIC_Y:orders}".to_string(), 5),
            ("${TOPIC_Z}".to_string(), 6)
        ]
    );
    assert_eq!(fi.declared_unread, 3);
    let dump = format!("{fi:?}").to_ascii_lowercase();
    for canary in [&weak, &weak_prod] {
        assert!(
            !dump.contains(canary.as_str()),
            "{canary} reached the index"
        );
    }
}

#[test]
fn a_bare_callee_matches_any_receiver_and_the_bare_call_but_not_a_shadowing_local() {
    let calls = vec![call("enqueue", 0, &[], &[])];
    let code = "const a = () => enqueue('one');\n\
                svc.enqueue('two');\n\
                function g(enqueue: any) { enqueue('three'); }\n";
    let fi = index_with(calls, "svc/b.ts", code);
    assert_eq!(
        producers(&fi),
        vec![("one".to_string(), 1), ("two".to_string(), 2)]
    );
}

#[test]
fn a_name_that_is_not_static_is_counted_and_the_other_key_still_yields() {
    let code = format!(
        "{ENUMS}export function f(jobType: string) {{\n  return JobsOutbox.createJob({{ type: jobType, \
         target: Queue.STORAGE }});\n}}\n\
         export const g = (props: object) => JobsOutbox.createJob(props);\n\
         export const h = () => JobsOutbox.createJob({{ type: TABLE[kind], target: Queue.MAIL }});\n"
    );
    let fi = index_with(outbox(), "svc/dyn.ts", &code);
    assert_eq!(
        producers(&fi),
        vec![
            ("Queue.STORAGE".to_string(), 4),
            ("Queue.MAIL".to_string(), 7)
        ]
    );
    // `type: jobType`, the argument that is not an object literal, `type: TABLE[kind]`.
    assert_eq!(fi.declared_unread, 3);
}

#[test]
fn a_missing_key_and_a_missing_argument_are_neither_a_producer_nor_unread() {
    let code = format!("{ENUMS}JobsOutbox.createJob({{ payload: 1 }});\nJobsOutbox.createJob();\n");
    let fi = index_with(outbox(), "svc/none.ts", &code);
    assert_eq!(producers(&fi), vec![]);
    assert_eq!(fi.declared_unread, 0);
}

#[test]
fn a_secret_shaped_name_never_becomes_a_topic() {
    // Built at runtime: a key-shaped literal in the source would trip a secret scanner.
    let bearer = ["Bearer", "_abcdef0123456789"].concat();
    let weak = ["hunter", "2-prod-queue"].concat();
    let token = ["tok", "_live_51Habcdef0123456789"].concat();
    let code = format!(
        "{ENUMS}JobsOutbox.createJob({{ type: '{bearer}', target: '{weak}' }});\n\
         JobsOutbox.createJob({{ type: '{token}' }});\n\
         JobsOutbox.createJob({{ type: 'password-reset-jobs' }});\n\
         JobsOutbox.createJob({{ type: Secrets.API_KEY }});\n"
    );
    let fi = index_with(outbox(), "svc/secrets.ts", &code);
    assert_eq!(producers(&fi), vec![("password-reset-jobs".to_string(), 5)]);
    assert_eq!(fi.declared_unread, 4);
    let dump = format!("{fi:?}");
    for canary in [&bearer, &weak, &token] {
        assert!(
            !dump.contains(canary.as_str()),
            "{canary} reached the index"
        );
    }
}

#[test]
fn sites_past_the_per_file_cap_are_counted() {
    let mut code = String::from(ENUMS);
    for i in 0..2_050 {
        code.push_str(&format!("JobsOutbox.createJob({{ type: 'job-{i}' }});\n"));
    }
    let fi = index_with(outbox(), "svc/many.ts", &code);
    assert_eq!(fi.producers.len(), 2_000);
    assert!(fi.declared_capped);
    let small = index_with(
        outbox(),
        "svc/few.ts",
        &format!("{ENUMS}JobsOutbox.createJob({{ type: 'x-job' }});\n"),
    );
    assert!(!small.declared_capped);
}

#[test]
fn nothing_is_read_without_a_declaration_and_a_broken_callee_is_skipped() {
    let code = format!("{ENUMS}JobsOutbox.createJob({{ type: JobType.A }});\n");
    assert_eq!(
        producers(&index_with(Vec::new(), "svc/x.ts", &code)),
        vec![]
    );
    for bad in ["", " ", "a b", "x.", ".createJob", "a..b", "createJob()"] {
        let fi = index_with(vec![call(bad, 0, &["type"], &[])], "svc/x.ts", &code);
        assert_eq!(producers(&fi), vec![], "callee {bad:?}");
    }
}

#[test]
fn the_declaration_changes_the_parse_cache_fingerprint_input() {
    let with = ExtractConfig {
        producer_calls: outbox(),
        ..ExtractConfig::default()
    };
    assert_ne!(
        format!("{with:?}"),
        format!("{:?}", ExtractConfig::default())
    );
}
