# Security policy

## Supported versions

This repository is a learning and experimentation project without versioned
releases. Security fixes apply to the current `main` branch.

## Report a vulnerability

Use GitHub's private vulnerability reporting form in the repository's
**Security** tab. Include the affected file or component, the impact, and the
smallest reproduction you can share safely.

Please keep suspected vulnerabilities out of public issues and pull requests
until a fix or disclosure plan is ready.

## Testing boundaries

Keep testing inside infrastructure and accounts you own or are authorized to
use. Do not test against another person's AWS account, deployed service, or
credentials. A local test credential found in `.env.example` or the Compose
stack is intentionally non-secret; report any credential that grants access to
a real system.
