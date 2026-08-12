# Security policy

## Supported versions

Until the first stable release, security fixes are applied to the latest release only.

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting for this repository. Do not open a public issue for a suspected vulnerability.

Include the affected version, operating system, a minimal reproduction, impact, and any suggested mitigation. You can expect an acknowledgement within seven days.

## Deployment model

duckdoor accepts SQL and is designed for trusted local users. It is not an isolation boundary for hostile tenants. The default loopback binding is intentional; remote deployments require an authenticated TLS proxy, network isolation, and operating-system resource controls.
