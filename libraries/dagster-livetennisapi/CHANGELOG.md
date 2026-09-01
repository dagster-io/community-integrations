# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.0.1] - 2026-08-16

### Added

- `LiveTennisApiResource`, a `ConfigurableResource` wrapping the official
  `livetennisapi` SDK client (API key from config or the `LIVETENNISAPI_KEY`
  environment variable).
- Free-tier asset factories: `build_fixtures_asset`,
  `build_players_asset` (run-time configurable search) and
  `build_live_matches_asset`.
- Fully mocked test suite (httpx `MockTransport`; no network).
