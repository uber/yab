// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"

	"github.com/yarpc/yab/limiter"
	"github.com/yarpc/yab/statsd"
	"github.com/yarpc/yab/transport"

	"go.uber.org/zap"
)

var (
	errNegativeDuration = errors.New("duration cannot be negative")
	errNegativeMaxReqs  = errors.New("max requests cannot be negative")

	// using a global _quantiles slice and _percentileMap mainly for ease of testing, and not passing the same array
	// around to multiple functions
	_quantiles     = []float64{0.5, 0.9, 0.95, 0.99, 0.999, 0.9995, 1.0}
	_percentileMap = map[float64]string{
		0.5:    "P50",
		0.9:    "P90",
		0.95:   "P95",
		0.99:   "P99",
		0.999:  "P999",
		0.9995: "P9995",
		1.0:    "P100",
	}
)

// BenchmarkParameters holds values of all benchmark parameters
type BenchmarkParameters struct {
	CPUs        int    `json:"cpus"`
	Concurrency int    `json:"concurrency"`
	Connections int    `json:"connections"`
	MaxRequests int    `json:"maxRequests"`
	MaxDuration string `json:"maxDuration"`
	MaxRPS      int    `json:"maxRPS"`
}

// KeyVal stores individual quantile -> latency value mapping
type latencyElement struct {
	quantile string
	latency  string
}

// LatencyMap stores multiple key-value pairs for latency value output to preserve specific ordering of elements.
type LatencyMap []latencyElement

// Summary stores the benchmarking summary
type Summary struct {
	ElapsedTime   string  `json:"elapsedTime"`
	TotalRequests int     `json:"totalRequests"`
	RPS           float64 `json:"rps"`
}

// BenchmarkOutput stores benchmark settings and results for JSON output
type BenchmarkOutput struct {
	BenchmarkParameters BenchmarkParameters `json:"benchmarkParameters"`
	Latencies           LatencyMap          `json:"latencies"`
	Summary             Summary             `json:"summary"`
}

// MarshalJSON implements the json.Marshaler interface for the custom latencyMap
func (latencyMap LatencyMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteString("{")
	for i, kv := range latencyMap {
		if i != 0 {
			buf.WriteString(",")
		}
		// marshal key
		key, err := json.Marshal(kv.quantile)
		if err != nil {
			return nil, err
		}

		buf.Write(key)
		buf.WriteString(":")
		// marshal value
		val, err := json.Marshal(kv.latency)
		if err != nil {
			return nil, err
		}
		buf.Write(val)
	}

	buf.WriteString("}")
	return buf.Bytes(), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface for the custom latencyMap
func (latencyMap *LatencyMap) UnmarshalJSON(b []byte) error {
	var unmarshalOutput map[string]string
	err := json.Unmarshal(b, &unmarshalOutput)
	if err != nil {
		return err
	}

	for k, v := range unmarshalOutput {
		kv := latencyElement{
			quantile: k,
			latency:  v,
		}
		*latencyMap = append(*latencyMap, kv)
	}

	return nil
}

// setGoMaxProcs sets runtime.GOMAXPROCS if the option is set
// and returns the number of GOMAXPROCS configured.
func (o BenchmarkOptions) setGoMaxProcs() int {
	if o.NumCPUs > 0 {
		runtime.GOMAXPROCS(o.NumCPUs)
	}
	return runtime.GOMAXPROCS(-1)
}

func (o BenchmarkOptions) getNumConnections(goMaxProcs int) int {
	if o.Connections > 0 {
		return o.Connections
	}

	// If the user doesn't specify a number of connections, choose a sane default.
	return goMaxProcs * 2
}

func (o BenchmarkOptions) validate() error {
	if o.MaxDuration < 0 {
		return errNegativeDuration
	}
	if o.MaxRequests < 0 {
		return errNegativeMaxReqs
	}

	return nil
}

func (o BenchmarkOptions) enabled() bool {
	// By default, benchmarks are disabled. At least MaxDuration or MaxRequests
	// should not be 0 for the benchmark to start.
	// We guard for negative values in the options validate() method, called
	// after entering the benchmark case.
	return o.MaxDuration != 0 || o.MaxRequests != 0
}

func runWorker(t transport.Transport, m benchmarkMethod, s *benchmarkState, run *limiter.Run, logger *zap.Logger) {
	for cur := run; cur.More(); {
		latency, err := m.call(t)
		if err != nil {
			s.recordError(err)
			// TODO: Add information about which peer specifically failed.
			logger.Info("Failed while making call.", zap.Error(err))
			continue
		}

		s.recordLatency(latency)
	}
}

func runBenchmark(out output, logger *zap.Logger, allOpts Options, resolved resolvedProtocolEncoding, m benchmarkMethod) {
	opts := allOpts.BOpts

	if err := opts.validate(); err != nil {
		out.Fatalf("Invalid benchmarking options: %v", err)
	}
	if !opts.enabled() {
		return
	}

	if opts.RPS > 0 && opts.MaxDuration > 0 {
		// The RPS * duration in seconds may cap opts.MaxRequests.
		rpsMax := int(float64(opts.RPS) * opts.MaxDuration.Seconds())
		if rpsMax < opts.MaxRequests || opts.MaxRequests == 0 {
			opts.MaxRequests = rpsMax
		}
	}

	goMaxProcs := opts.setGoMaxProcs()
	numConns := opts.getNumConnections(goMaxProcs)

	benchmarkParameters := BenchmarkParameters{
		CPUs:        goMaxProcs,
		Connections: numConns,
		Concurrency: opts.Concurrency,
		MaxRequests: opts.MaxRequests,
		MaxDuration: opts.MaxDuration.String(),
		MaxRPS:      opts.RPS,
	}

	formatAsJSON := false
	if opts.Format == "json" || opts.Format == "JSON" {
		formatAsJSON = true
	} else if opts.Format != "" {
		out.Printf("Unrecognized format option <%s>, please specify <json> or <JSON> for JSON output. Printing plaintext output as default.\n\n", opts.Format)
	}

	if !formatAsJSON {
		printBenchmarkParameters(out, benchmarkParameters)
	}
	// If format is JSON, benchmark parameters are printed after benchmark is run to maintain a single JSON blob

	// Warm up number of connections.
	logger.Debug("Warming up connections.", zap.Int("numConns", numConns))
	connections, err := m.WarmTransports(numConns, allOpts.TOpts, resolved, opts.WarmupRequests)
	if err != nil {
		out.Fatalf("Failed to warmup connections for benchmark: %v", err)
	}

	globalStatter, err := statsd.NewClient(logger, opts.StatsdHostPort, allOpts.TOpts.ServiceName, m.Method())
	if err != nil {
		out.Fatalf("Failed to create statsd client for benchmark: %v", err)
	}

	var wg sync.WaitGroup
	states := make([]*benchmarkState, len(connections)*opts.Concurrency)

	for i, c := range connections {
		statter := globalStatter

		if opts.PerPeerStats {
			// If per-peer stats are enabled, dual emit metrics to the original value
			// and the per-peer value.
			prefix := fmt.Sprintf("peer.%v.", c.peerID)
			statter = statsd.MultiClient(
				statter,
				statsd.NewPrefixedClient(statter, prefix),
			)
		}

		for j := 0; j < opts.Concurrency; j++ {
			states[i*opts.Concurrency+j] = newBenchmarkState(statter)
		}
	}

	run := limiter.New(opts.MaxRequests, opts.RPS, opts.MaxDuration)
	stopOnInterrupt(out, run)

	logger.Info("Benchmark starting.", zap.Any("options", opts))
	start := time.Now()
	for i, c := range connections {
		for j := 0; j < opts.Concurrency; j++ {
			state := states[i*opts.Concurrency+j]

			wg.Add(1)
			go func(c transport.Transport) {
				defer wg.Done()
				runWorker(c, m, state, run, logger)
			}(c)
		}
	}

	// Wait for all the worker goroutines to end.
	wg.Wait()
	total := time.Since(start)
	// Merge all the states into 0
	overall := states[0]
	for _, s := range states[1:] {
		overall.merge(s)
	}

	logger.Info("Benchmark complete.",
		zap.Duration("totalDuration", total),
		zap.Int("totalRequests", overall.totalRequests),
		zap.Time("startTime", start),
	)

	// Print out errors
	// TODO: allow errors to be printed in JSON format for output consistency
	overall.printErrors(out)

	latencyValues := overall.getLatencies()

	summary := Summary{
		ElapsedTime:   (total / time.Millisecond * time.Millisecond).String(),
		TotalRequests: overall.totalRequests,
		RPS:           float64(overall.totalRequests) / total.Seconds(),
	}

	if formatAsJSON {
		outputJSON(out, benchmarkParameters, latencyValues, summary)
	} else {
		outputPlaintext(out, latencyValues, summary)
	}
}

// JSON output helper method
func outputJSON(out output, benchmarkParameters BenchmarkParameters, latencyValues map[float64]time.Duration, summary Summary) {

	latencies := LatencyMap{}
	for _, quantile := range _quantiles {
		element := latencyElement{
			quantile: _percentileMap[quantile],
			latency:  latencyValues[quantile].String(),
		}
		latencies = append(latencies, element)
	}

	// create BenchmarkOutput struct
	benchmarkOutput := BenchmarkOutput{
		BenchmarkParameters: benchmarkParameters,
		Latencies:           latencies,
		Summary:             summary,
	}

	jsonOutput, err := json.MarshalIndent(&benchmarkOutput, "" /* prefix */, "  " /* indent */)
	if err != nil {
		out.Fatalf("Failed to marshal benchmark output: %v\n", err)
	}

	out.Printf("%s\n\n", jsonOutput)
}

// Plaintext output helper method
func outputPlaintext(out output, latencyValues map[float64]time.Duration, summary Summary) {
	// Print out latencies
	printLatencies(out, latencyValues)

	// Print out summary
	out.Printf("Elapsed time:      %v\n", summary.ElapsedTime)
	out.Printf("Total requests:    %v\n", summary.TotalRequests)
	out.Printf("RPS:               %.2f\n", summary.RPS)
}

func printBenchmarkParameters(out output, benchmarkParameters BenchmarkParameters) {
	out.Printf("Benchmark parameters:\n")
	out.Printf("  CPUs:            %v\n", benchmarkParameters.CPUs)
	out.Printf("  Connections:     %v\n", benchmarkParameters.Connections)
	out.Printf("  Concurrency:     %v\n", benchmarkParameters.Concurrency)
	out.Printf("  Max requests:    %v\n", benchmarkParameters.MaxRequests)
	out.Printf("  Max duration:    %v\n", benchmarkParameters.MaxDuration)
	out.Printf("  Max RPS:         %v\n", benchmarkParameters.MaxRPS)
}

func printLatencies(out output, latencyValues map[float64]time.Duration) {
	out.Printf("Latencies:\n")
	for _, quantile := range _quantiles {
		out.Printf("  %.4f: %v\n", quantile, latencyValues[quantile])
	}
}

// stopOnInterrupt sets up a signal that will trigger the run to stop.
func stopOnInterrupt(out output, r *limiter.Run) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		<-c
		// Preceding newline since Ctrl-C will be printed inline.
		out.Printf("\n!!Benchmark interrupted!!\n")
		r.Stop()
	}()
}
