FROM ghcr.io/anomalyco/opencode:2.0.7

# AgentBox uses Bash for setup, Python for its manifest diff, and curl for
# authenticated guest-local readiness probes; the upstream image omits them.
RUN apk add --no-cache bash curl python3 coreutils git
WORKDIR /workspace
