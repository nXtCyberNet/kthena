---
title: Accurate Token Counting via Sidecar Tokenizer Service
authors:
- "@nXtCyberNet" # Authors' GitHub accounts here.
reviewers:
- "@hzxuzhonghu"
approvers:
- "@hzxuzhonghu"

creation-date: 2026-06-16

---

## Accurate Token Counting via Sidecar Tokenizer Service
<!--
This is the title of your proposal. Keep it short, simple, and descriptive. A good
title can help communicate what the proposal is and should be considered as part of
any review.
-->

### Summary
The current token rate limiter was using the len(prompt) / 4 heuristic to estimate input tokens , which creates significant inaccuracy for billing, compliance, and strict pre-request rate limiting.

This proposal introduces an optional sidecar tokenizer service (containerized Python application using HuggingFace's tokenizers library) that operators can deploy alongside the router for accurate token counting.

<!--
This section is incredibly important for producing high-quality, user-focused
documentation such as release notes or a development roadmap.

A good summary is probably at least a paragraph in length.
-->

### Motivation
The SimpleEstimateTokenizer works acceptably for latency-sensitive development environments but breaks down for production billing and compliance paths:
- Billing Accuracy Problem - The /4 heuristic estimates can diverge ±30-40% from actual token counts depending on model architecture and content type.
- Compliance Requirements - Regulated environments require auditable, deterministic token accounting that matches model-reported usage. Heuristic estimates cannot satisfy compliance or audit requirements.

<!--
This section is for explicitly listing the motivation, goals, and non-goals of
this proposal.  Describe why the change is important and the benefits to users.
-->

#### Goals

- Provide accurate token counting for billing, compliance, and predictable quota enforcement without forcing all operators to pay the latency cost

- Support model-agnostic tokenization across HuggingFace Hub, local PVC-mounted models, ModelShare and ConfigMap-based tokenizer configs
-Maintain backward compatibility: existing ModelRoute behavior unchanged.
-Keep latency predictable: sidecar operates in-pod (HTTP over localhost).

<!--
List the specific goals of the proposal. What is it trying to achieve? How will we
know that this has succeeded?
-->

#### Non-Goals
- Support for third-party MaaS tokenization (OpenAI, Anthropic) — these already return token usage in responses. 

<!--
What is out of scope for this proposal? Listing non-goals helps to focus discussion
and make progress.
-->


### Proposal

#### Core Design Principles

- **Global Sidecar Toggle** – A Helm value (`kthenaRouter.tokenizerSidecar.enabled`) controls whether the tokenizer sidecar is deployed in the router pod.  
  - **If disabled**: the sidecar is omitted; the router auto‑detects its absence and uses the `len(prompt)/4` heuristic for **all** models.  
  - **If enabled**: the sidecar runs (idle) and is activated only for models with the annotation.

- **Per‑Model Activation** – A **single annotation** on the `ModelServer` (`kthena.volcano.sh/tokenizer-enabled: "true"`) tells the router to use accurate tokenization for that specific model. No new CRD fields.

- **Auto‑Detection & Graceful Degradation** – The router periodically pings the sidecar’s health endpoint (`/v1/health`). If the sidecar is unavailable (missing, crashed, or unreachable), the router automatically falls back to the heuristic – **no manual intervention** needed.

- **Event‑Driven Loading** – The router’s existing `store.RegisterCallback` watches `ModelServer` events. When a model with the annotation is added/updated, it publishes a Redis message; the sidecar subscribes and loads the tokenizer. When the annotation is removed or the model is deleted, it publishes an unload message.

- **HTTP‑Only Encode API** – The sidecar exposes only `/v1/encode` (and `/v1/health`). All load/unload coordination happens via Redis.

---

#### Lifecycle Flow

1. **Helm Deployment**  
   - If `tokenizerSidecar.enabled: false` in `values.yaml`, the sidecar container is **not** included in the router pod. The router starts, detects it’s missing, and logs that it will use the heuristic.

2. **Operator Creates/Updates a `ModelServer`**  
   ```yaml
  apiVersion: networking.serving.volcano.sh/v1alpha1
  kind: ModelServer
  metadata:
    name: deepseek-r1-7b
    namespace: default
    annotations:
      kthena.volcano.sh/tokenizer-enabled: "true"
  spec:
    workloadSelector:
      matchLabels:
        app: deepseek-r1-7b
    workloadPort:
      port: 8000
    model: "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B"
    inferenceEngine: "vLLM"
    trafficPolicy:
      timeout: 10s
   ```

   - The router’s `store.RegisterCallback` for `ModelServer` detects the event.
   - If the annotation is `"true"` **and** the sidecar is available (health check passed), the callback publishes a Redis message on the `tokenizer-events` channel:

     ```json
     {
       "action": "load",
       "model_server_id": "qwen-3.5-server",
       "model_id": "hf:Qwen/Qwen3.5-397B-A17B"
     }
     ```

   - If the annotation is missing or set to any value other than `"true"`, the callback publishes an unload message (if previously loaded).

3. **Sidecar (Python + FastAPI)** subscribes to `tokenizer-events` and loads the tokenizer:
   - For `hf:` prefixes – downloads `tokenizer.json` from HuggingFace Hub.
   - For `local:` prefixes – reads from a mounted PVC.
   - The tokenizer is cached in‑memory (`model_server_id → Tokenizer`).
   - It optionally updates a Redis hash (`tokenizer-status`) with `"loaded"` or an error message.

4. **Controller Reconciliation** (optional enhancement)  
   - The controller can poll Redis to update `ModelServer.status.tokenizationReady` and `tokenizationError`.

5. **Client Request Arrives**  
   - The **RateLimiter** checks if the target `model_server_id` is enabled **and** the sidecar is available.
   - If yes, it makes a `POST /v1/encode` request with the prompt.
   - The sidecar returns the token count; the rate limiter deducts it from the user’s quota.
   - If the sidecar is unavailable or the call fails, it **falls back to `len(prompt)/4`** (with a warning log).

6. **ModelServer Deletion or Annotation Removal**  
   - The callback publishes an `unload` Redis message; the sidecar removes the tokenizer from its cache.

---

#### Global Flag & Auto‑Detection

- The global flag is a **Helm value** – it does **not** require a CRD change.
- its requires 2 fields - 
```
kthenaRouter:
  tokenizerSidecar:
    enabled: true
    ```
- The router performs a health check on startup and every 30 seconds:
  ```go
  http.Get("http://localhost:50051/v1/health")
  ```
  - If successful → sidecar is available; the router will use it for annotated models.
  - If it fails (connection refused, timeout, non‑200) → sidecar is marked unavailable; the router uses the heuristic for **all** models.
- This means operators can **disable the sidecar entirely** in Helm for resource‑constrained environments, and the router adapts seamlessly.

---

#### Annotation – Only Per‑Model Control

The annotation is the **only** piece of configuration that an operator touches per model:

```yaml
metadata:
  annotations:
    kthena.volcano.sh/tokenizer-enabled: "true"   # enable accurate counting
```

- If absent → router uses heuristic for that model (even if the sidecar is running).
- If `"true"` → router uses the sidecar (provided it is available).

---

#### Rate Limiter Integration 

- The rate limiter now checks **both** the model’s annotation (via `tokenizerManager.IsEnabled(modelServerID)`) **and** the sidecar’s availability before attempting an RPC. If either condition fails, it falls back to the heuristic.
---

#### Helm Configuration Summary

```yaml
# values.yaml
kthenaRouter:
  tokenizerSidecar:
    enabled: false   # ← global toggle; if false, sidecar omitted
    image: tokenizer-sidecar:latest
    resources:
      requests: { memory: "128Mi", cpu: "50m" }
      limits:   { memory: "1Gi", cpu: "500m" }
```

- When `enabled: false` – the router auto‑detects the missing sidecar and logs: `"Tokenizer sidecar not deployed, using heuristic"`.
- When `enabled: true` – the sidecar is deployed; the router activates it per‑model via annotations.
---

<!--
This is where we get down to the specifics of what the proposal actually is.
This should have enough detail that reviewers can understand exactly what
you're proposing, but should not include things like API designs or
implementation. What is the desired outcome and how do we measure success?.
The "Design Details" section below is for the real
nitty-gritty.
-->

### Story 1 Enable accurate token counting for a production model

A PaaS operator runs a multi-tenant LLM service. They need accurate token counts for their flagship qwen-3.5-397b model to ensure billing compliance and pass financial audits.



### Story 2 Disable the sidecar entirely for resource-constrained development clusters

A development team runs Kthena on a small, resource-constrained cluster (e.g., local Kind or minikube). They do not require accurate token counting for their testing workloads and want to minimize memory/CPU overhead.The operator sets a global Helm value to false . The router performs a health check on startup; when the sidecar is absent, it logs a clear message and seamlessly falls back to the native len(prompt) / 4 estimator for all models—even if individual ModelServer resources have the annotation (the annotation is safely ignored).

#### Notes/Constraints/Caveats (Optional)
- Memory Limits : Each loaded tokenizer consumes approximately 50–200MB of memory. The sidecar uses an LRU cache with a configurable max size (default: 5 tokenizers). Operators must set appropriate resources.limits.memory (default 1Gi) to avoid OOM kills.

- Helm Rollout Behavior : Changing the global Helm value tokenizerSidecar.enabled from true to false triggers a rolling restart of the router pods. During the rollout, tokenization may be temporarily unavailable for some pods (they fall back to the heuristic) until all pods are recreated.
- Annotation Strictness : The annotation value must be exactly the string "true" (case-sensitive). Any other value ("True", "yes", "1") is treated as false, and the model uses the heuristic. This is a deliberate safe default.

<!--
What are the caveats to the proposal?
What are some important details that didn't come across above?
Go in to as much detail as necessary here.
This might be a good place to talk about core concepts and how they relate.
-->

#### Risks and Mitigations

<!--
What are the risks of this proposal, and how do we mitigate?

How will security be reviewed, and by whom?

How will UX be reviewed, and by whom?

Consider including folks who also work outside the SIG or subproject.
-->

### Design Details
Component Changes

    Router (kthena-router):

        New package: pkg/tokenizer/ – contains Manager with:

            Health checker (periodic GET /v1/health).

            Enabled model cache (populated from annotations via Redis events).

            Redis publisher for load/unload events.

        Modified RateLimiter.CheckQuota() to call Manager.IsEnabled() and sidecar HTTP endpoint.

        Enhanced store.RegisterCallback for ModelServer to check annotation and publish Redis events.

    Sidecar (Python + FastAPI):

        New container in the router pod (conditional via Helm).

        Endpoints:

            POST /v1/encode – expects {"model_server_id": "...", "text": "..."}.

            GET /v1/health – returns {"status": "ok"}.

        Background thread: Redis subscriber listening to tokenizer-events.

        Tokenizer loader using huggingface_hub or local file.

    Helm Chart:

        New value: kthenaRouter.tokenizerSidecar.enabled (default false).
                Conditional container inclusion.


API Details

Redis Message Format (published to tokenizer-events):
```json

{
  "action": "load",
  "model_server_id": "qwen-3.5-server",
  "model_id": "hf:Qwen/Qwen3.5-397B-A17B",
}

Sidecar HTTP /v1/encode Request/Response:

// Request
{
  "model_server_id": "qwen-3.5-server",
  "text": "Hello, world!",
  "return_tokens": false
}
// Response (success)
{
  "token_count": 4,
  "token_ids": null
}
// Response (error)
{
  "detail": "Tokenizer not loaded for server qwen-3.5-server"
}
```

<!--
This section should contain enough information that the specifics of your
change are understandable. This may include API specs (though not always
required) or even code snippets. If there's any ambiguity about HOW your
proposal will be implemented, this is the place to discuss them.
-->

#### Test Plan
Unit Tests

    Tokenizer Manager:
        - IsEnabled() returns correct value for models.
        - Health check updates availability flag.
        - Redis publish/unpublish called on annotation changes.

    Sidecar:
        - Tokenizer loads from HF Hub (mock download).
        - Tokenizer loads from local path.
        - /v1/encode returns correct token count.
        - /v1/encode returns 404 if model not loaded.
        - Redis subscriber processes load/unload messages

End-to-End Scenario

 - Deploy a full Kthena stack (router + vLLM backend).
 - Enable tokenization for one model via annotation.
 - Send prompts of varying content (code, natural language, mixed).
 -  Compare sidecar token counts against the model's official transformers tokenizer.
 - Verify quota deductions match the sidecar counts.
 - Remove annotation; verify fallback to heuristic.
 - Disable sidecar via Helm; verify auto-detection logs and heuristic behaviour.

<!--
**Note:** *Not required until targeted at a release.*

Consider the following in developing a test plan for this enhancement:
- Will there be e2e and integration tests, in addition to unit tests?
- How will it be tested in isolation vs with other components?

No need to outline all test cases, just the general strategy. Anything
that would count as tricky in the implementation, and anything particularly
challenging to test, should be called out.

-->
### Alternatives

Alternative 1 — Adding a spec field to ModelServer instead of an annotation
Why rejected:
- Requires a CRD schema change, forcing a cluster-wide upgrade and potential controller regeneration.
- Annotations are simpler, optional, and do not affect the CRD API version.

