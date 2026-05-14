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

package router

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientset "github.com/volcano-sh/kthena/client-go/clientset/versioned"
	networkingv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/networking/v1alpha1"
	routercontext "github.com/volcano-sh/kthena/test/e2e/router/context"
	"github.com/volcano-sh/kthena/test/e2e/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

func logWebhookError(t *testing.T, prefix string, err error) {
	t.Helper()

	if err == nil {
		return
	}

	t.Logf("%s Error Type: %T", prefix, err)
	t.Logf("%s Error String: %s", prefix, err.Error())
	t.Logf("%s Error Full: %+v", prefix, err)
}

func isRetryableWebhookReadinessError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	return strings.Contains(errStr, "connect: connection refused") ||
		strings.Contains(errStr, "i/o timeout") ||
		strings.Contains(errStr, "context deadline exceeded") ||
		strings.Contains(errStr, "no endpoints available") ||
		strings.Contains(errStr, "service unavailable") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "EOF")
}

// waitForKthenaRouterValidatingWebhook polls until a DryRun ModelRoute create reaches the
// validating webhook (avoids flaky tests while cert-manager / deployment finishes).
func waitForKthenaRouterValidatingWebhook(t *testing.T, ctx context.Context, kthenaClient *clientset.Clientset, namespace string) {
	t.Helper()

	const (
		readinessTimeout  = 2 * time.Minute
		readinessInterval = 2 * time.Second
	)

	t.Log("[WEBHOOK_READINESS] ╔════════════════════════════════════════════════════════════╗")
	t.Log("[WEBHOOK_READINESS] ║ Waiting for kthena-router validating webhook              ║")
	t.Log("[WEBHOOK_READINESS] ╚════════════════════════════════════════════════════════════╝")
	t.Logf("[WEBHOOK_READINESS] Namespace: %s", namespace)
	t.Logf("[WEBHOOK_READINESS] Timeout: %s", readinessTimeout)
	t.Logf("[WEBHOOK_READINESS] Poll Interval: %s", readinessInterval)

	weight100 := uint32(100)
	waitCtx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()

	start := time.Now()
	attempt := 0
	var lastErr error

	err := wait.PollUntilContextCancel(waitCtx, readinessInterval, true, func(ctx context.Context) (bool, error) {
		attempt++

		elapsed := time.Since(start).Round(time.Millisecond)
		probeName := "webhook-ready-probe-" + utils.RandomString(5)

		t.Logf("[WEBHOOK_READINESS] ┌─ Attempt %d | elapsed=%s", attempt, elapsed)
		t.Logf("[WEBHOOK_READINESS] │ Probe ModelRoute: %s/%s", namespace, probeName)

		probe := &networkingv1alpha1.ModelRoute{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      probeName,
			},
			Spec: networkingv1alpha1.ModelRouteSpec{
				ModelName: "probe-model",
				Rules: []*networkingv1alpha1.Rule{
					{
						Name: "default",
						TargetModels: []*networkingv1alpha1.TargetModel{
							{ModelServerName: routercontext.ModelServer1_5bName, Weight: &weight100},
						},
					},
				},
			},
		}

		_, err := kthenaClient.NetworkingV1alpha1().
			ModelRoutes(namespace).
			Create(ctx, probe, metav1.CreateOptions{DryRun: []string{"All"}})

		if err != nil {
			lastErr = err

			if isRetryableWebhookReadinessError(err) {
				t.Logf("[WEBHOOK_READINESS] │ Result: retryable error")
				logWebhookError(t, "[WEBHOOK_READINESS] │", err)
				t.Log("[WEBHOOK_READINESS] └─ Retrying")
				return false, nil
			}

			t.Logf("[WEBHOOK_READINESS] │ Result: non-retryable error")
			logWebhookError(t, "[WEBHOOK_READINESS] │", err)
			t.Log("[WEBHOOK_READINESS] └─ Failing")
			return false, err
		}

		t.Logf("[WEBHOOK_READINESS] │ Result: webhook accepted DryRun request")
		t.Logf("[WEBHOOK_READINESS] └─ Ready after %d attempt(s), elapsed=%s", attempt, time.Since(start).Round(time.Millisecond))
		return true, nil
	})

	if err != nil {
		t.Log("[WEBHOOK_READINESS] ════════════════════════════════════════════════════════════")
		t.Log("[WEBHOOK_READINESS] FINAL FAILURE")
		t.Logf("[WEBHOOK_READINESS] Namespace: %s", namespace)
		t.Logf("[WEBHOOK_READINESS] Attempts: %d", attempt)
		t.Logf("[WEBHOOK_READINESS] Elapsed: %s", time.Since(start).Round(time.Millisecond))

		if lastErr != nil {
			t.Log("[WEBHOOK_READINESS] Last observed error:")
			logWebhookError(t, "[WEBHOOK_READINESS]", lastErr)
		}

		t.Log("[WEBHOOK_READINESS] ════════════════════════════════════════════════════════════")
	}

	require.NoError(t, err, "kthena-router validating webhook did not become ready in time")
}

// TestKthenaRouterValidatingWebhook ensures the networking chart's ValidatingWebhookConfiguration
// targets the real API group and the router webhook rejects invalid ModelRoute specs.
// Invalid case uses an empty string in loraAdapters (CRD CEL allows non-empty list; webhook rejects item).
func TestKthenaRouterValidatingWebhook(t *testing.T) {
	ctx := context.Background()
	testStart := time.Now()

	t.Log("[TEST] ╔════════════════════════════════════════════════════════════╗")
	t.Log("[TEST] ║ TestKthenaRouterValidatingWebhook                         ║")
	t.Log("[TEST] ╚════════════════════════════════════════════════════════════╝")
	t.Logf("[TEST] Start Time: %s", testStart.Format(time.RFC3339Nano))
	t.Logf("[TEST] Test Namespace: %s", testNamespace)

	t.Log("[TEST] ┌─ STEP 1/3: Wait for router validating webhook readiness")
	waitForKthenaRouterValidatingWebhook(t, ctx, testCtx.KthenaClient, testNamespace)
	t.Log("[TEST] └─ STEP 1/3 completed")

	weight100 := uint32(100)

	validRouteName := "webhook-valid-dryrun-" + utils.RandomString(5)
	t.Log("[TEST] ┌─ STEP 2/3: Create valid ModelRoute with DryRun")
	t.Logf("[TEST] │ Valid ModelRoute: %s/%s", testNamespace, validRouteName)

	validRoute := &networkingv1alpha1.ModelRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      validRouteName,
		},
		Spec: networkingv1alpha1.ModelRouteSpec{
			ModelName: "webhook-valid",
			Rules: []*networkingv1alpha1.Rule{
				{
					Name: "default",
					TargetModels: []*networkingv1alpha1.TargetModel{
						{ModelServerName: routercontext.ModelServer1_5bName, Weight: &weight100},
					},
				},
			},
		},
	}

	_, err := testCtx.KthenaClient.NetworkingV1alpha1().
		ModelRoutes(testNamespace).
		Create(ctx, validRoute, metav1.CreateOptions{DryRun: []string{"All"}})

	if err != nil {
		t.Log("[TEST] │ Valid ModelRoute DryRun failed")
		logWebhookError(t, "[TEST] │", err)
	} else {
		t.Log("[TEST] │ Valid ModelRoute DryRun succeeded")
	}

	require.NoError(t, err, "expected validating webhook to allow a valid ModelRoute (DryRun)")
	t.Log("[TEST] └─ STEP 2/3 completed")

	invalidRouteName := "webhook-invalid-dryrun-" + utils.RandomString(5)
	t.Log("[TEST] ┌─ STEP 3/3: Create invalid ModelRoute with DryRun")
	t.Logf("[TEST] │ Invalid ModelRoute: %s/%s", testNamespace, invalidRouteName)
	t.Log("[TEST] │ Expected rejection reason: lora adapter name cannot be an empty string")

	invalidRoute := &networkingv1alpha1.ModelRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      invalidRouteName,
		},
		Spec: networkingv1alpha1.ModelRouteSpec{
			ModelName:    "",
			LoraAdapters: []string{""},
			Rules: []*networkingv1alpha1.Rule{
				{
					Name: "default",
					TargetModels: []*networkingv1alpha1.TargetModel{
						{ModelServerName: routercontext.ModelServer1_5bName, Weight: &weight100},
					},
				},
			},
		},
	}

	_, err = testCtx.KthenaClient.NetworkingV1alpha1().
		ModelRoutes(testNamespace).
		Create(ctx, invalidRoute, metav1.CreateOptions{DryRun: []string{"All"}})

	if err != nil {
		t.Log("[TEST] │ Invalid ModelRoute DryRun was rejected")
		logWebhookError(t, "[TEST] │", err)
	} else {
		t.Log("[TEST] │ Invalid ModelRoute DryRun unexpectedly succeeded")
	}

	require.Error(t, err, "expected validating webhook to reject invalid ModelRoute")

	expectedErr := "lora adapter name cannot be an empty string"
	if err != nil {
		t.Logf("[TEST] │ Checking error contains: %q", expectedErr)
		t.Logf("[TEST] │ Actual error: %q", err.Error())
	}

	assert.Contains(t, err.Error(), expectedErr)
	t.Log("[TEST] └─ STEP 3/3 completed")

	t.Log("[TEST] ════════════════════════════════════════════════════════════")
	t.Logf("[TEST] PASSED in %s", time.Since(testStart).Round(time.Millisecond))
	t.Log("[TEST] ════════════════════════════════════════════════════════════")
}
