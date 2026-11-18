### Fixed
- Tests: `doctor_repairable_health` and `stdio_resilience` locate the `mesh-mcp` binary through `CARGO_BIN_EXE_mesh-mcp` instead of a path derived from the test executable, which broke on the current stable Cargo (the binary was reported missing on all three CI platforms).
