// Package repro captures Go execution using build-time instrumentation.
package repro

import (
	_ "github.com/getrepro/go-sdk/instrumentation"
	"github.com/getrepro/go-sdk/internal/capture"
	"github.com/getrepro/go-sdk/internal/privacy"
)

type Config = capture.Config
type Limits = capture.Limits
type Runtime = capture.Runtime
type PrivacyConfig = privacy.Config
type LiveConfig = capture.LiveConfig
type LivePolicy = capture.LivePolicy
type Target = capture.Target
type TracingConfig = capture.TracingConfig

func Init(config Config) (*Runtime, error) { return capture.Init(config) }
