# Documentation review

The documentation uses the `simple-english` skill in pragmatic mode.
No separate skill named “Simple English Review Analyzer” was available during implementation.

## Review scope

The review covers the README, architecture, API, test cases, operations, and performance documents.
Code, CLI flags, Protobuf identifiers, and quoted output retain their exact spelling.

## Review criteria

- Procedural sentences use direct instructions and a maximum of 20 words.
- Descriptive sentences use a maximum of 25 words.
- Each paragraph covers one topic.
- Conditions precede the instructions that depend on them.
- Terms remain consistent: job, attempt, lease, worker, coordinator, and configuration.
- Commands match the implemented CLI.
- Claims distinguish measured results from design intent.

## Corrections

| Rule | Earlier wording | Final wording |
|---|---|---|
| 3.2 | “Workers should renew their leases.” | “Workers renew the lease every second.” |
| 4.2 | “You can watch it while it’s running.” | “The client receives progress through `WatchJob`.” |
| 5.4 | “Restart workers if they stop.” | “If the workers stop, restart them.” |
| 1.11 | “Settings” and “configuration” for the same document | “Configuration” throughout the prose |

The final pass includes sentence length, contractions, filler, terminology, local links, and command examples.
This review is an editorial aid. It does not certify compliance with the official ASD-STE100 dictionary.
