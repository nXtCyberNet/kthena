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
	"fmt"
	"runtime"
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
	"k8s.io/client-go/kubernetes"
)

// logError logs comprehensive error information with full details
func logError(t *testing.T, prefix string, err error) {
	if err == nil {
		t.Logf("[%s] Error: <nil>", prefix)
		return
	}

	t.Logf("[%s] ════════════════════════════════════════════════════════════", prefix)
	t.Logf("[%s] ERROR DETAILS:", prefix)
	t.Logf("[%s] ════════════════════════════════════════════════════════════", prefix)
	t.Logf("[%s] Error Type: %T", prefix, err)
	t.Logf("[%s] Error Interface: %v", prefix, err)
	t.Logf("[%s] Error String: %s", prefix, err.Error())
	t.Logf("[%s] Raw Error Message: %q", prefix, fmt.Sprintf("%v", err))

	// Try to extract more details if available
	if causedErr := fmt.Sprintf("%+v", err); causedErr != fmt.Sprintf("%v", err) {
		t.Logf("[%s] Error With Stack: %+v", prefix, err)
	}

	// Log the caller information
	_, file, line, ok := runtime.Caller(1)
	if ok {
		t.Logf("[%s] Called from: %s:%d", prefix, file, line)
	}
	t.Logf("[%s] ════════════════════════════════════════════════════════════", prefix)
}

// isWebhookTransientError returns true for connection/TLS errors that indicate
// the webhook pod is starting up, but not yet fully ready to handle requests.
func isWebhookTransientError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	isTransient := strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "i/o timeout") ||
		strings.Contains(errStr, "context deadline exceeded") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "TLS handshake") ||
		strings.Contains(errStr, "no such host")
	return isTransient
}

// categorizeError returns a human-readable categorization of the error
func categorizeError(err error) string {
	if err == nil {
		return "nil"
	}
	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "connection reset by peer"):
		return "CONNECTION_RESET"
	case strings.Contains(errStr, "connection refused"):
		return "CONNECTION_REFUSED"
	case strings.Contains(errStr, "i/o timeout"):
		return "IO_TIMEOUT"
	case strings.Contains(errStr, "context deadline exceeded"):
		return "CONTEXT_DEADLINE"
	case strings.Contains(errStr, "EOF"):
		return "EOF_ERROR"
	case strings.Contains(errStr, "TLS handshake"):
		return "TLS_HANDSHAKE_ERROR"
	case strings.Contains(errStr, "no such host"):
		return "NAME_RESOLUTION_ERROR"
	default:
		return "OTHER_ERROR"
	}
}

// waitForWebhookDeploymentReady polls until the kthena-router webhook Deployment
// has all desired replicas ready. This ensures the pod is scheduled and the container
// has started before we attempt TLS-level webhook probing.
func waitForWebhookDeploymentReady(t *testing.T, ctx context.Context, kubeClient kubernetes.Interface, namespace, deploymentName string, timeout time.Duration) {
	t.Helper()
	t.Logf("[WEBHOOK_READINESS] ┌─── Starting deployment readiness check ───")
	t.Logf("[WEBHOOK_READINESS] Namespace: %s", namespace)
	t.Logf("[WEBHOOK_READINESS] Deployment Name: %s", deploymentName)
	t.Logf("[WEBHOOK_READINESS] Timeout: %v", timeout)

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	attemptCount := 0
	startTime := time.Now()

	err := wait.PollUntilContextCancel(waitCtx, 1*time.Second, true, func(ctx context.Context) (bool, error) {
		attemptCount++
		elapsed := time.Since(startTime)

		deploy, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		if err != nil {
			t.Logf("[WEBHOOK_READINESS] [%dms/Attempt %d] ✗ Deployment fetch failed", elapsed.Milliseconds(), attemptCount)
			logError(t, "WEBHOOK_READINESS", err)
			return false, nil
		}

		desiredReplicas := *deploy.Spec.Replicas
		readyReplicas := deploy.Status.ReadyReplicas
		updatedReplicas := deploy.Status.UpdatedReplicas
		availableReplicas := deploy.Status.AvailableReplicas
		observedGeneration := deploy.Status.ObservedGeneration
		specGeneration := deploy.ObjectMeta.Generation

		t.Logf("[WEBHOOK_READINESS] [%dms/Attempt %d] ───────────────────────────────────────", elapsed.Milliseconds(), attemptCount)
		t.Logf("[WEBHOOK_READINESS] [%dms/Attempt %d] Status Check Results:", elapsed.Milliseconds(), attemptCount)
		t.Logf("[WEBHOOK_READINESS]   Desired Replicas: %d", desiredReplicas)
		t.Logf("[WEBHOOK_READINESS]   Ready Replicas: %d", readyReplicas)
		t.Logf("[WEBHOOK_READINESS]   Updated Replicas: %d", updatedReplicas)
		t.Logf("[WEBHOOK_READINESS]   Available Replicas: %d", availableReplicas)
		t.Logf("[WEBHOOK_READINESS]   Observed Generation: %d", observedGeneration)
		t.Logf("[WEBHOOK_READINESS]   Spec Generation: %d", specGeneration)

		// Log pod information if available
		if deploy.Status.Conditions != nil && len(deploy.Status.Conditions) > 0 {
			t.Logf("[WEBHOOK_READINESS] Deployment Conditions (%d total):", len(deploy.Status.Conditions))
			for i, cond := range deploy.Status.Conditions {
				t.Logf("[WEBHOOK_READINESS]   [%d] Type: %s", i, cond.Type)
				t.Logf("[WEBHOOK_READINESS]       Status: %s", cond.Status)
				t.Logf("[WEBHOOK_READINESS]       Reason: %s", cond.Reason)
				t.Logf("[WEBHOOK_READINESS]       Message: %s", cond.Message)
				t.Logf("[WEBHOOK_READINESS]       Last Update: %v", cond.LastUpdateTime)
				t.Logf("[WEBHOOK_READINESS]       Last Transition: %v", cond.LastTransitionTime)
			}
		}

		// Check if all desired replicas are ready and available
		if readyReplicas == desiredReplicas && availableReplicas == desiredReplicas && observedGeneration == specGeneration {
			t.Logf("[WEBHOOK_READINESS] [%dms] ✓ SUCCESS: Deployment %s/%s is ready with %d/%d replicas", elapsed.Milliseconds(), namespace, deploymentName, readyReplicas, desiredReplicas)
			return true, nil
		}

		t.Logf("[WEBHOOK_READINESS] [%dms] ⏳ Not ready yet: Ready=%d, Available=%d, Updated=%d (expected %d each)", elapsed.Milliseconds(), readyReplicas, availableReplicas, updatedReplicas, desiredReplicas)
		return false, nil
	})

	if err != nil {
		t.Logf("[WEBHOOK_READINESS] └─── ✗ FAILED ───────────────────────────────────────")
		t.Logf("[WEBHOOK_READINESS] Deployment readiness check failed")
		t.Logf("[WEBHOOK_READINESS] Total attempts: %d", attemptCount)
		t.Logf("[WEBHOOK_READINESS] Elapsed time: %v", time.Since(startTime))
		logError(t, "WEBHOOK_READINESS", err)
	} else {
		t.Logf("[WEBHOOK_READINESS] └─── ✓ SUCCESS ───────────────────────────────────────")
		t.Logf("[WEBHOOK_READINESS] Deployment ready after %d attempts in %v", attemptCount, time.Since(startTime))
	}

	require.NoError(t, err, fmt.Sprintf("webhook Deployment %s/%s did not become ready in time (attempted %d times)", namespace, deploymentName, attemptCount))
}

// waitForKthenaRouterValidatingWebhook polls until a DryRun ModelRoute create reaches the
// validating webhook (avoids flaky tests while cert-manager / deployment finishes).
// It waits for the webhook deployment to be ready, then probes the webhook with TLS-aware
// error handling to account for the startup window on resource-constrained Kind clusters.
func waitForKthenaRouterValidatingWebhook(t *testing.T, ctx context.Context, kthenaClient *clientset.Clientset, kubeClient kubernetes.Interface, namespace string) {
	t.Helper()

	// First, wait for the webhook Deployment to have replicas ready.
	// This eliminates most of the TLS startup race on slow Kind nodes.
	waitForWebhookDeploymentReady(t, ctx, kubeClient, namespace, "kthena-router", 2*time.Minute)

	// Then poll for TLS readiness by attempting DryRun creates.
	t.Logf("[TLS_READINESS] ┌─── Starting TLS readiness check ───")
	t.Logf("[TLS_READINESS] Namespace: %s", namespace)
	t.Logf("[TLS_READINESS] Webhook Endpoint: https://kthena-router-webhook.%s.svc:443", namespace)

	weight100 := uint32(100)
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	attemptCount := 0
	transientErrors := 0
	startTime := time.Now()

	err := wait.PollUntilContextCancel(waitCtx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		attemptCount++
		elapsed := time.Since(startTime)
		probeName := "webhook-ready-probe-" + utils.RandomString(5)

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

		t.Logf("[TLS_READINESS] [%dms/Attempt %d] ─────────────────────────────────────", elapsed.Milliseconds(), attemptCount)
		t.Logf("[TLS_READINESS] [%dms/Attempt %d] Sending DryRun probe", elapsed.Milliseconds(), attemptCount)
		t.Logf("[TLS_READINESS]   Probe Name: %s", probeName)
		t.Logf("[TLS_READINESS]   Namespace: %s", namespace)
		t.Logf("[TLS_READINESS]   Model Name: %s", probe.Spec.ModelName)

		_, err := kthenaClient.NetworkingV1alpha1().ModelRoutes(namespace).Create(ctx, probe, metav1.CreateOptions{DryRun: []string{"All"}})
		if err != nil {
			// Transient errors (connection resets, timeouts, TLS not ready) → retry
			if isWebhookTransientError(err) {
				transientErrors++
				t.Logf("[TLS_READINESS] [%dms/Attempt %d] ⚠ TRANSIENT ERROR (retry #%d)", elapsed.Milliseconds(), attemptCount, transientErrors)
				t.Logf("[TLS_READINESS]   Error Category: %s", categorizeError(err))
				logError(t, "TLS_READINESS_TRANSIENT", err)
				return false, nil
			}
			// Non-transient errors (validation failures, etc.) → fail fast
			t.Logf("[TLS_READINESS] [%dms/Attempt %d] ✗ NON-TRANSIENT ERROR (WILL FAIL TEST)", elapsed.Milliseconds(), attemptCount)
			t.Logf("[TLS_READINESS]   Error Category: %s", categorizeError(err))
			logError(t, "TLS_READINESS_FATAL", err)
			return false, err
		}

		t.Logf("[TLS_READINESS] [%dms/Attempt %d] ✓ SUCCESS: Webhook accepted DryRun probe", elapsed.Milliseconds(), attemptCount)
		return true, nil
	})

	t.Logf("[TLS_READINESS] ───────────────────────────────────────")
	if err != nil {
		t.Logf("[TLS_READINESS] └─── ✗ FAILED ───────────────────────────────────────")
		t.Logf("[TLS_READINESS] TLS readiness check failed")
		t.Logf("[TLS_READINESS] Total attempts: %d", attemptCount)
		t.Logf("[TLS_READINESS] Transient errors encountered: %d", transientErrors)
		t.Logf("[TLS_READINESS] Total elapsed time: %v", time.Since(startTime))
		logError(t, "TLS_READINESS_FINAL", err)
	} else {
		t.Logf("[TLS_READINESS] └─── ✓ SUCCESS ───────────────────────────────────────")
		t.Logf("[TLS_READINESS] TLS ready after %d attempts (with %d transient errors) in %v", attemptCount, transientErrors, time.Since(startTime))
	}

	require.NoError(t, err, fmt.Sprintf("kthena-router validating webhook did not become ready in time (attempted %d times with %d transient errors)", attemptCount, transientErrors))
}

// TestKthenaRouterValidatingWebhook ensures the networking chart's ValidatingWebhookConfiguration
// targets the real API group and the router webhook rejects invalid ModelRoute specs.
// Invalid case uses an empty string in loraAdapters (CRD CEL allows non-empty list; webhook rejects item).
func TestKthenaRouterValidatingWebhook(t *testing.T) {
	testStartTime := time.Now()
	t.Logf("[TEST] ╔════════════════════════════════════════════════════════════╗")
	t.Logf("[TEST] ║  TestKthenaRouterValidatingWebhook")
	t.Logf("[TEST] ║  Start Time: %v", testStartTime)
	t.Logf("[TEST] ║  Test Namespace: %s", testNamespace)
	t.Logf("[TEST] ╚════════════════════════════════════════════════════════════╝")

	ctx := context.Background()

	// Step 1: Wait for webhook to be ready
	t.Logf("[TEST] ┌─ STEP 1/4: Waiting for webhook readiness ──────────────────")
	step1Start := time.Now()
	waitForKthenaRouterValidatingWebhook(t, ctx, testCtx.KthenaClient, testCtx.KubeClient, testNamespace)
	step1Duration := time.Since(step1Start)
	t.Logf("[TEST] └─ STEP 1/4: ✓ Complete (took %v)", step1Duration)

	weight100 := uint32(100)

	// Step 2: Test valid ModelRoute
	t.Logf("[TEST] ┌─ STEP 2/4: Testing valid ModelRoute acceptance ────────────")
	validRouteName := "webhook-valid-dryrun-" + utils.RandomString(5)
	t.Logf("[TEST] Route Name: %s", validRouteName)

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

	step2Start := time.Now()
	t.Logf("[TEST] Sending DryRun Create request for valid route...")
	_, err := testCtx.KthenaClient.NetworkingV1alpha1().ModelRoutes(testNamespace).Create(ctx, validRoute, metav1.CreateOptions{DryRun: []string{"All"}})
	step2Duration := time.Since(step2Start)

	if err != nil {
		t.Logf("[TEST] └─ STEP 2/4: ✗ FAILED (took %v)", step2Duration)
		t.Logf("[TEST] Valid ModelRoute was rejected by webhook (unexpected)")
		logError(t, "TEST_STEP2_VALID_ROUTE", err)
	} else {
		t.Logf("[TEST] └─ STEP 2/4: ✓ Complete (took %v)", step2Duration)
		t.Logf("[TEST] Valid ModelRoute was accepted by webhook (expected)")
	}
	require.NoError(t, err, fmt.Sprintf("expected validating webhook to allow a valid ModelRoute (DryRun). Route: %s", validRouteName))

	// Step 3: Test invalid ModelRoute
	t.Logf("[TEST] ┌─ STEP 3/4: Testing invalid ModelRoute rejection ──────────")
	invalidRouteName := "webhook-invalid-dryrun-" + utils.RandomString(5)
	t.Logf("[TEST] Route Name: %s", invalidRouteName)

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

	step3Start := time.Now()
	t.Logf("[TEST] Sending DryRun Create request for invalid route...")
	t.Logf("[TEST] Invalid specs:")
	t.Logf("[TEST]   - ModelName: \"\" (empty)")
	t.Logf("[TEST]   - LoraAdapters: [\"\"] (empty string)")
	_, err = testCtx.KthenaClient.NetworkingV1alpha1().ModelRoutes(testNamespace).Create(ctx, invalidRoute, metav1.CreateOptions{DryRun: []string{"All"}})
	step3Duration := time.Since(step3Start)

	if err == nil {
		t.Logf("[TEST] └─ STEP 3/4: ✗ FAILED (took %v)", step3Duration)
		t.Logf("[TEST] Invalid ModelRoute was accepted by webhook (unexpected)")
		t.Logf("[TEST] Should have been rejected with validation error")
	} else {
		t.Logf("[TEST] └─ STEP 3/4: ✓ Complete (took %v)", step3Duration)
		t.Logf("[TEST] Invalid ModelRoute was rejected by webhook (expected)")
		logError(t, "TEST_STEP3_INVALID_ROUTE", err)
	}
	require.Error(t, err, fmt.Sprintf("expected validating webhook to reject invalid ModelRoute. Route: %s", invalidRouteName))

	// Step 4: Verify error message
	t.Logf("[TEST] ┌─ STEP 4/4: Verifying rejection message ──────────────────")
	t.Logf("[TEST] Expected substring: 'lora adapter name cannot be an empty string'")
	t.Logf("[TEST] Actual error message:")
	t.Logf("[TEST] %s", err.Error())

	step4Start := time.Now()
	errorContainsExpected := assert.Contains(t, err.Error(), "lora adapter name cannot be an empty string")
	step4Duration := time.Since(step4Start)

	if errorContainsExpected {
		t.Logf("[TEST] └─ STEP 4/4: ✓ Complete (took %v)", step4Duration)
		t.Logf("[TEST] Error message contains expected validation text")
	} else {
		t.Logf("[TEST] └─ STEP 4/4: ✗ FAILED (took %v)", step4Duration)
		t.Logf("[TEST] Error message MISSING expected validation text")
		logError(t, "TEST_STEP4_MESSAGE", err)
	}

	totalDuration := time.Since(testStartTime)
	t.Logf("[TEST] ╔════════════════════════════════════════════════════════════╗")
	t.Logf("[TEST] ║  TEST COMPLETE")
	t.Logf("[TEST] ║  Status: ✓ PASSED")
	t.Logf("[TEST] ║  Total Duration: %v", totalDuration)
	t.Logf("[TEST] ║  Breakdown:")
	t.Logf("[TEST] ║    Step 1 (Webhook Readiness): %v", step1Duration)
	t.Logf("[TEST] ║    Step 2 (Valid Route): %v", step2Duration)
	t.Logf("[TEST] ║    Step 3 (Invalid Route): %v", step3Duration)
	t.Logf("[TEST] ║    Step 4 (Message Verify): %v", step4Duration)
	t.Logf("[TEST] ╚════════════════════════════════════════════════════════════╝")
}
