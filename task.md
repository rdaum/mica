# Tasks

- [x] Refactor Driver dispatch path to async
    - [x] Make submit_source, submit_source_report, submit_source_as_actor, submit_invocation, resume, and input async
    - [x] Keep endpoint open/close operations synchronous
    - [x] Handle recursive handle_outcome with non-allocating loop
    - [x] Update mica-runner to use async driver and block_on
    - [x] Update mica-telnet-host to use async driver
    - [x] Update mica-daemon to use async driver
    - [x] Fix all tests in the workspace and clippy warnings
- [ ] Relational HTTP Routing Demo
    - [ ] Augment web-host to assert RequestActor facts
    - [ ] Implement examples/relational-router.mica
    - [ ] Verify stratified negation for security policies
- [x] Performance Validation
    - [x] Fix latency script readiness and xargs behaviour
    - [x] Measure latency post-refactor
    - [ ] Establish a real pre/post benchmark baseline
