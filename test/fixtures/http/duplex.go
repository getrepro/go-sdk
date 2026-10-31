package main

import (
	"bytes"
	"io"
	"net/http"
)

type protocolBody struct{ *bytes.Buffer }

func (*protocolBody) Close() error { return nil }

type upgradeTransport struct{ body *protocolBody }

func (t upgradeTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 101, Header: make(http.Header), Body: t.body, ContentLength: -1}, nil
}

func duplexWorkflow() {
	body := &protocolBody{Buffer: bytes.NewBufferString("upgrade-")}
	client := &http.Client{Transport: upgradeTransport{body: body}}
	response, err := client.Get("http://owned-stream.test/upgrade")
	check(err == nil, "upgrade failed")
	writer, ok := response.Body.(io.Writer)
	check(ok, "instrumentation removed upgraded response io.Writer")
	_, err = writer.Write([]byte("sent"))
	check(err == nil, "upgrade write failed")
	data, err := io.ReadAll(response.Body)
	check(err == nil && string(data) == "upgrade-sent", "upgrade bytes changed")
	_ = response.Body.Close()
}
