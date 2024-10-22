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

package transport

import (
	"bytes"
	"errors"
	"fmt"
	"golang.org/x/net/http2"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
)

type httpTransport struct {
	opts   HTTPOptions
	client *http.Client
	tracer opentracing.Tracer
}

// HTTPOptions are used to create a HTTP transport.
type HTTPOptions struct {
	Method          string
	URLs            []string
	SourceService   string
	TargetService   string
	RoutingDelegate string
	RoutingKey      string
	ShardKey        string
	Encoding        string
	Tracer          opentracing.Tracer
}

var (
	errNoURLs        = errors.New("specify at least one URL")
	errMissingTarget = errors.New("specify target service name")
)

// NewHTTP returns a transport that calls a HTTP service.
func NewHTTP(opts HTTPOptions) (Transport, error) {
	if len(opts.URLs) == 0 {
		return nil, errNoURLs
	}
	if opts.TargetService == "" {
		return nil, errMissingTarget
	}
	if opts.Method == "" {
		opts.Method = "POST"
	}

	return &httpTransport{
		opts: opts,
		// Use independent HTTP clients for each transport.
		client: &http.Client{
			Transport: &http2.Transport{},
		},
		tracer: opts.Tracer,
	}, nil
}

func (h *httpTransport) Tracer() opentracing.Tracer {
	return h.tracer
}

func (h *httpTransport) newReq(ctx context.Context, r *Request) (*http.Request, error) {
	url := h.opts.URLs[rand.Intn(len(h.opts.URLs))]

	// TODO: We should envelope Thrift payloads here.
	req, err := http.NewRequest(h.opts.Method, url, bytes.NewReader(r.Body))
	if err != nil {
		return nil, err
	}

	timeout := time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = deadline.Sub(time.Now())
	}

	// TODO: We shouldn't always set YARPC headers, bit maybe have a flag to enable these.
	h.applyRPCHeaders(req.Header, r, req, timeout)

	for key, val := range r.TransportHeaders {
		req.Header.Add(key, val)
	}

	span := opentracing.SpanFromContext(ctx)
	if span != nil && h.tracer != nil {
		h.tracer.Inject(
			span.Context(),
			opentracing.HTTPHeaders,
			opentracing.HTTPHeadersCarrier(req.Header),
		)
	}

	return req, nil
}

func (h *httpTransport) applyRPCHeaders(headers http.Header, r *Request, req *http.Request, timeout time.Duration) {
	req.Header.Add("RPC-Service", h.opts.TargetService)
	req.Header.Add("RPC-Procedure", r.Method)
	req.Header.Add("RPC-Caller", h.opts.SourceService)
	req.Header.Add("RPC-Encoding", h.opts.Encoding)
	if h.opts.RoutingKey != "" {
		req.Header.Add("RPC-Routing-Key", h.opts.RoutingKey)
	}
	if h.opts.RoutingDelegate != "" {
		req.Header.Add("RPC-Routing-Delegate", h.opts.RoutingDelegate)
	}
	if h.opts.ShardKey != "" {
		req.Header.Add("RPC-Shard-Key", h.opts.ShardKey)
	}
	req.Header.Add("Context-TTL-MS", strconv.Itoa(int(timeout/time.Millisecond)))

	for key, val := range r.Headers {
		req.Header.Add("Rpc-Header-"+key, val)
	}
}

func (h *httpTransport) Protocol() Protocol {
	return HTTP
}

func (h *httpTransport) Call(ctx context.Context, r *Request) (*Response, error) {
	req, err := h.newReq(ctx, r)
	if err != nil {
		return nil, err
	}

	resp, err := h.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP call got non-success response code: %v, body: %s", resp.StatusCode, body)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read HTTP response body: %v", err)
	}

	headers := make(map[string]string)
	for headerKey := range resp.Header {
		headers[headerKey] = resp.Header.Get(headerKey)
	}

	return &Response{
		Headers: headers,
		Body:    body,
		TransportFields: map[string]interface{}{
			"statusCode": resp.StatusCode,
		},
	}, nil
}
