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
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"github.com/yarpc/yab/transport"

	"github.com/jessevdk/go-flags"
	"github.com/uber/tchannel-go"
)

var errHealthAndMethod = errors.New("cannot specify method name and use --health")

func findGroup(parser *flags.Parser, group string) *flags.Group {
	if g := parser.Group.Find(group); g != nil {
		return g
	}

	panic("no group called " + group + " found.")
}

func fromPositional(args []string, index int, s *string) bool {
	if len(args) <= index {
		return false
	}

	if args[index] != "" {
		*s = args[index]
	}

	return true
}

func main() {
	log.SetFlags(0)
	parseAndRun(consoleOutput{os.Stdout})
}

// parseAndRun is like main, but uses the given output.
func parseAndRun(out output) {
	var opts Options
	parser := flags.NewParser(&opts, flags.HelpFlag|flags.PassDoubleDash)
	parser.Usage = "[<service> <method> <body>] [OPTIONS]"
	parser.ShortDescription = "yet another benchmarker"
	parser.LongDescription = `
yab is a benchmarking tool for TChannel and HTTP applications. It's primarily intended for Thrift applications but supports other encodings like JSON and binary (raw).

It can be used in a curl-like fashion when benchmarking features are disabled.
`

	findGroup(parser, "transport").ShortDescription = "Transport Options"
	findGroup(parser, "request").ShortDescription = "Request Options"
	findGroup(parser, "benchmark").ShortDescription = "Benchmark Options"

	// If there are no arguments specified, write the help.
	if len(os.Args) <= 1 {
		parser.WriteHelp(out)
		return
	}

	remaining, err := parser.Parse()
	if err != nil {
		if ferr, ok := err.(*flags.Error); ok {
			if ferr.Type == flags.ErrHelp {
				parser.WriteHelp(out)
				return
			}
		}
		out.Fatalf("Failed to parse flags: %v", err)
	}

	if opts.DisplayVersion {
		out.Printf("yab version %v\n", versionString)
		return
	}

	if opts.ManPage {
		parser.WriteManPage(os.Stdout)
		return
	}

	fromPositional(remaining, 0, &opts.TOpts.ServiceName)
	fromPositional(remaining, 1, &opts.ROpts.MethodName)

	// We support both:
	// [service] [method] [request]
	// [service] [method] [headers] [request]
	if fromPositional(remaining, 3, &opts.ROpts.RequestJSON) {
		fromPositional(remaining, 2, &opts.ROpts.HeadersJSON)
	} else {
		fromPositional(remaining, 2, &opts.ROpts.RequestJSON)
	}

	runWithOptions(opts, out)
	return
}

func runWithOptions(opts Options, out output) {
	reqInput, err := getRequestInput(opts.ROpts.RequestJSON, opts.ROpts.RequestFile)
	if err != nil {
		out.Fatalf("Failed while loading body input: %v\n", err)
	}

	headers, err := getHeaders(opts.ROpts.HeadersJSON, opts.ROpts.HeadersFile)
	if err != nil {
		out.Fatalf("Failed while loading headers input: %v\n", err)
	}

	serializer, err := NewSerializer(opts.ROpts)
	if err != nil {
		out.Fatalf("Failed while parsing input: %v\n", err)
	}

	// transport abstracts the underlying wire protocol used to make the call.
	transport, err := getTransport(opts.TOpts, serializer.Encoding())
	if err != nil {
		out.Fatalf("Failed while parsing options: %v\n", err)
	}

	// req is the transport.Request that will be used to make a call.
	req, err := serializer.Request(reqInput)
	if err != nil {
		out.Fatalf("Failed while parsing request input: %v\n", err)
	}

	req.Headers = headers
	req.Timeout = opts.ROpts.Timeout.Duration()
	if req.Timeout == 0 {
		req.Timeout = time.Second
	}

	response, err := makeRequest(transport, req)
	if err != nil {
		out.Fatalf("Failed while making call: %v\n", err)
	}

	// responseMap converts the Thrift bytes response to a map.
	responseMap, err := serializer.Response(response)
	if err != nil {
		out.Fatalf("Failed while parsing response: %v\n", err)
	}

	// Print the initial output body.
	outSerialized := map[string]interface{}{
		"body": responseMap,
	}
	if len(response.Headers) > 0 {
		outSerialized["headers"] = response.Headers
	}
	if response.Trace != "" {
		outSerialized["trace"] = response.Trace
	}
	bs, err := json.MarshalIndent(outSerialized, "", "  ")
	if err != nil {
		out.Fatalf("Failed to convert map to JSON: %v\nMap: %+v\n", err, responseMap)
	}
	out.Printf("%s\n\n", bs)

	runBenchmark(out, opts, benchmarkMethod{
		serializer: serializer,
		req:        req,
	})
}

// makeRequest makes a request using the given transport.
func makeRequest(t transport.Transport, request *transport.Request) (*transport.Response, error) {
	ctx, cancel := tchannel.NewContext(request.Timeout)
	defer cancel()

	return t.Call(ctx, request)
}
