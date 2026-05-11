/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/time/rate"
	"k8s.io/klog/v2"

	networkingv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/networking/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/kthena-router/filters/tokenizer"
)

type RateLimitExceededError struct{}

func (e *RateLimitExceededError) Error() string {
	return "rate limit exceeded"
}

type InputRateLimitExceededError struct{}

func (e *InputRateLimitExceededError) Error() string {
	return "input token rate limit exceeded"
}

type OutputRateLimitExceededError struct{}

func (e *OutputRateLimitExceededError) Error() string {
	return "output token rate limit exceeded"
}

// Limiter interface that both local and global rate limiters implement
// Only includes methods that are actually used
type Limiter interface {
	// AllowN reports whether n tokens may be consumed and consumes them if so
	AllowN(now time.Time, n int) bool
	// Tokens returns the number of tokens currently available
	Tokens() float64
}

// LimiterConfig stores the configuration for a rate limiter to detect changes
type LimiterConfig struct {
	Limiter               Limiter
	InputTokensPerUnit    *uint32
	OutputTokensPerUnit   *uint32
	Unit                  networkingv1alpha1.RateLimitUnit
	GlobalRedisAddress    string // for detecting config changes
	LastUpdateTime        time.Time
}

// TokenRateLimiter provides rate limiting functionality for both input and output tokens
type TokenRateLimiter struct {
	mutex sync.RWMutex

	// Store both limiter and its config for change detection
	inputConfigs  map[string]*LimiterConfig
	outputConfigs map[string]*LimiterConfig

	// Redis client for global rate limiting
	redisClient *redis.Client

	tokenizer tokenizer.Tokenizer
}

// LocalLimiter wraps golang.org/x/time/rate.Limiter to implement our Limiter interface
type LocalLimiter struct {
	*rate.Limiter
}

// NewLocalLimiter creates a new LocalLimiter
func NewLocalLimiter(limit rate.Limit, burst int) *LocalLimiter {
	return &LocalLimiter{
		Limiter: rate.NewLimiter(limit, burst),
	}
}

// Tokens returns the number of tokens currently available
func (l *LocalLimiter) Tokens() float64 {
	return l.Limiter.Tokens()
}

// NewTokenRateLimiter creates a new TokenRateLimiter instance
func NewTokenRateLimiter() *TokenRateLimiter {
	return &TokenRateLimiter{
		inputConfigs:  make(map[string]*LimiterConfig),
		outputConfigs: make(map[string]*LimiterConfig),
		tokenizer:     tokenizer.NewSimpleEstimateTokenizer(),
	}
}

// RateLimit checks if the request is within rate limits for both input and output tokens
func (r *TokenRateLimiter) RateLimit(model, prompt string) error {
	// Estimate input tokens
	tokens, err := r.tokenizer.CalculateTokenNum(prompt)
	if err != nil {
		klog.Errorf("failed to calculate token number: %v", err)
		tokens = len(prompt) / 4 // fallback estimation
	}

	r.mutex.RLock()
	inputConfig, hasInputLimit := r.inputConfigs[model]
	outputConfig, hasOutputLimit := r.outputConfigs[model]
	r.mutex.RUnlock()

	// Check input token rate limit
	if hasInputLimit && inputConfig.Limiter != nil && !inputConfig.Limiter.AllowN(time.Now(), tokens) {
		return &InputRateLimitExceededError{}
	}

	// Check output token rate limit - we conservatively check if there's at least 1 token available
	// This prevents starting requests that likely won't be able to complete
	if hasOutputLimit && outputConfig.Limiter != nil && outputConfig.Limiter.Tokens() < 1.0 {
		return &OutputRateLimitExceededError{}
	}

	return nil
}

// RecordOutputTokens records the actual output tokens consumed after response generation
func (r *TokenRateLimiter) RecordOutputTokens(model string, tokenCount int) {
	r.mutex.RLock()
	outputConfig, exists := r.outputConfigs[model]
	r.mutex.RUnlock()

	if exists && outputConfig.Limiter != nil {
		outputConfig.Limiter.AllowN(time.Now(), tokenCount)
	}
}

// AddOrUpdateLimiter adds or updates rate limiter for a model
// Only recreates limiters if the configuration has actually changed,
// preserving limiter state across reconciliation events
func (r *TokenRateLimiter) AddOrUpdateLimiter(model string, ratelimit *networkingv1alpha1.RateLimit) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Determine if we should use global or local rate limiting
	useGlobal := ratelimit.Global != nil && ratelimit.Global.Redis != nil

	// Helper function to check if config has changed
	configChanged := func(oldConfig *LimiterConfig) bool {
		if oldConfig == nil {
			return true // No existing config, so it's "changed"
		}
		if oldConfig.Unit != ratelimit.Unit {
			return true
		}
		// Compare token limits
		oldInput := oldConfig.InputTokensPerUnit
		oldOutput := oldConfig.OutputTokensPerUnit
		if (oldInput == nil) != (ratelimit.InputTokensPerUnit == nil) {
			return true
		}
		if oldInput != nil && ratelimit.InputTokensPerUnit != nil && *oldInput != *ratelimit.InputTokensPerUnit {
			return true
		}
		if (oldOutput == nil) != (ratelimit.OutputTokensPerUnit == nil) {
			return true
		}
		if oldOutput != nil && ratelimit.OutputTokensPerUnit != nil && *oldOutput != *ratelimit.OutputTokensPerUnit {
			return true
		}
		return false
	}

	// Process input rate limiter
	if ratelimit.InputTokensPerUnit != nil {
		inputConfig, exists := r.inputConfigs[model]
		if !exists || configChanged(inputConfig) {
			// Config changed or doesn't exist - create new limiter
			var newLimiter Limiter
			var redisAddr string

			if useGlobal {
				// Initialize Redis client if not already done
				if r.redisClient == nil {
					r.redisClient = redis.NewClient(&redis.Options{
						Addr: ratelimit.Global.Redis.Address,
					})

					// Test connection
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := r.redisClient.Ping(ctx).Err(); err != nil {
						return fmt.Errorf("failed to connect to redis: %w", err)
					}
				}
				redisAddr = ratelimit.Global.Redis.Address
				newLimiter = NewGlobalRateLimiter(
					r.redisClient,
					"kthena:ratelimit",
					model,
					"input",
					*ratelimit.InputTokensPerUnit,
					ratelimit.Unit,
				)
			} else {
				// Create local rate limiter
				duration := getTimeUnitDuration(ratelimit.Unit)
				newLimiter = NewLocalLimiter(
					rate.Limit(float64(*ratelimit.InputTokensPerUnit)/duration.Seconds()),
					int(*ratelimit.InputTokensPerUnit),
				)
			}

			r.inputConfigs[model] = &LimiterConfig{
				Limiter:            newLimiter,
				InputTokensPerUnit: ratelimit.InputTokensPerUnit,
				Unit:               ratelimit.Unit,
				GlobalRedisAddress: redisAddr,
				LastUpdateTime:     time.Now(),
			}
		}
		// If config hasn't changed, keep existing limiter with its state
	} else {
		// Input rate limit removed
		delete(r.inputConfigs, model)
	}

	// Process output rate limiter
	if ratelimit.OutputTokensPerUnit != nil {
		outputConfig, exists := r.outputConfigs[model]
		if !exists || configChanged(outputConfig) {
			// Config changed or doesn't exist - create new limiter
			var newLimiter Limiter
			var redisAddr string

			if useGlobal {
				// Initialize Redis client if not already done
				if r.redisClient == nil {
					r.redisClient = redis.NewClient(&redis.Options{
						Addr: ratelimit.Global.Redis.Address,
					})

					// Test connection
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := r.redisClient.Ping(ctx).Err(); err != nil {
						return fmt.Errorf("failed to connect to redis: %w", err)
					}
				}
				redisAddr = ratelimit.Global.Redis.Address
				newLimiter = NewGlobalRateLimiter(
					r.redisClient,
					"kthena:ratelimit",
					model,
					"output",
					*ratelimit.OutputTokensPerUnit,
					ratelimit.Unit,
				)
			} else {
				// Create local rate limiter
				duration := getTimeUnitDuration(ratelimit.Unit)
				newLimiter = NewLocalLimiter(
					rate.Limit(float64(*ratelimit.OutputTokensPerUnit)/duration.Seconds()),
					int(*ratelimit.OutputTokensPerUnit),
				)
			}

			r.outputConfigs[model] = &LimiterConfig{
				Limiter:             newLimiter,
				OutputTokensPerUnit: ratelimit.OutputTokensPerUnit,
				Unit:                ratelimit.Unit,
				GlobalRedisAddress:  redisAddr,
				LastUpdateTime:      time.Now(),
			}
		}
		// If config hasn't changed, keep existing limiter with its state
	} else {
		// Output rate limit removed
		delete(r.outputConfigs, model)
	}

	return nil
}

// DeleteLimiter deletes rate limiter for a model
func (r *TokenRateLimiter) DeleteLimiter(model string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.inputConfigs, model)
	delete(r.outputConfigs, model)
}

func getTimeUnitDuration(unit networkingv1alpha1.RateLimitUnit) time.Duration {
	switch unit {
	case networkingv1alpha1.Second:
		return time.Second
	case networkingv1alpha1.Minute:
		return time.Minute
	case networkingv1alpha1.Hour:
		return time.Hour
	case networkingv1alpha1.Day:
		return 24 * time.Hour
	case networkingv1alpha1.Month:
		return 30 * 24 * time.Hour // Approximate
	default:
		return time.Second
	}
}
