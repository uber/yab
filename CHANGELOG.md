Changelog
=========

# 0.17.0 (2020-03-24)
* Fix gRPC reflection requests not propogating routing key and routing delegate.
* When Thrift method is not found, list all available methdods in service::method
  to match the passed-in method format.
* Improved errors when Proto service is not found by including
  list of all available services.

# 0.16.1 (2019-10-02)
* Fix bug that prevented nested JSON to be marshalled into Proto encoded
  requests.
* Fix bug that required users to explictly specify encoding using `-e` when
  making health requests. Now health requests default to Proto when using gRPC
  and Thrift when using TChannel.

# 0.16.0 (2019-09-09)
* Add support for grpc/proto when using `--health`.
* Fix bug that prevented yab templates to work with proto. Simple YAML yab
  templates now work with proto, though not all template substitutions are
  supported yet.

# 0.15.0 (2019-07-07)
* Allow `null` in request templates to imply skipping the field.
* Allow specifying HTTP method for HTTP requests using `--http-method`.
* Add support for URLs in peer lists.
* Add support for per-peer stats from benchmarks using `--per-peer-stats`.
* Fix bug where benchmark stats were missing procedure if `--health` was used.

# 0.14.3 (2019-04-16)
* Fix bug where values specified in templates as `"y"` were being
  converted to true.

# 0.14.2 (2019-01-31)
* Fix grpc reflection stream being closed abnormally causing
  the reflection server to see an unexpected client disconnect.

# 0.14.1 (2018-10-22)
* Fix a bug for incorrectly dialing the reflection server when
  peers contain the `grpc://` scheme instead of just host:port pairs.

# 0.14.0 (2018-10-18)
* Support protobuf encoding using:
  - Precompiled FileDescriptorSets with flags `--file-descriptor-set-bin`
    or `-F`.
  - Using the grpc reflection API if no descriptor set is specified.
* Encoding now defaults to `proto` if the method is of form
  `package.Service/Method`.

# 0.13.0 (2018-06-14)
* Add Jaeger throttling config.

# 0.12.0 (2017-11-16)
* Add gRPC+Thrift support. Protobuf is not yet supported.
* Allow build-time plugins to register middleware for request modification.
* Allow build-time plugins to register custom CLI flags.

# 0.11.0 (2017-09-06)
* Support using yab as a shebang for files ending with `.yab`.

# 0.10.2 (2017-08-29)
* Fix timeouts specified in templates being ignored.

# 0.10.1 (2017-04-24)
* Fix for HTTP not respecting context timeout.

# 0.10.0 (2017-04-07)

* Support disabling Thrift envelopes from YAML templates.
* Add template argument support, which allows passing parameters
  to YAML templates.

# 0.9.0 (2017-02-28)

* Support for custom peer providers to provide a list of peers such as
  over HTTP.
* Support more options in YAML templates such peers, baggage, and other
  transport headers.
* Rename method to procedure, and maintain aliases for `--method`.
* Allow TChannel peers to be specified using a URI with a TChannel scheme,
  e.g., `tchannel://host:port`.

# 0.8.0 (2017-02-13)

* Support for YAML request templates with CLI overrides.
* Add --no-jaeger to disable Jaeger.

# 0.7.0 (2016-11-03)

* Upgrade to TChannel 1.2.0 and add Open Tracing support.
* Support set constants in Thrift files.
* Add support for headers with flags like -H key:value.
* Add support for baggage headers with flags like -B key:value when baggage
  propagation is enabled with --jaeger.
* Add support for routing key, routing delegate, and shard key, for both
  HTTP and TChannel, using --rk, --rd, and --sk flags.

# 0.6.2 (2016-09-22)

* Add error rate to benchmarking output.
* Improve boolean parsing for Thrift input.
* Check the system XDG directory for a config file.
* Allow unlimited duration or requests when benchmarking. (#105)

# 0.6.1 (2016-08-26)

* Improve default format detection:
  - If `-t` is specified, assume Thrift
  - Otherwise, assume JSON
* Improved support for quoted strings as JSON payloads.
* Add RPC-Encoding header for HTTP requests for compatibility
  with the latest version of YARPC.
* Any peers specified on the command-line should override all
  peer options set in the defaults.ini. (#104)

# 0.6.0 (2016-08-01)

* Allow JSON to be used with non-map requests and responses.
* Expose HTTP response body on non-OK responses.
* Expose TChannel call status ("ok") and HTTP status code ("code"
  on successful responses.
* Fix ini parsing of option groups. (#82)
* Add support for TMultiplexedProtocol when using HTTP+Thrift using
  `--multiplexed-thrift`.

# 0.5.4 (2016-07-20)

* Fix for benchmarking taking longer than duration at low RPS. (#73)

# 0.5.2 (2016-07-18)

* Fix `--peer-list` not loading the peer list. Regression from 0.5.0. (#70)

# 0.5.0 (2016-07-18)

* Support for reading default options using XDG base directories.
* Round robin peer-selection when creating connections for benchmarking.
* Allow disabling Thrift envelopes for HTTP using `--disable-thrift-envelope`.

# 0.4.0 (2016-07-01)

* First beta release.
