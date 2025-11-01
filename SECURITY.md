# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Cloud9, please report it privately to our security team.

**Do not** create a public GitHub issue for security vulnerabilities.

### How to Report

Email: <security@dedaluslabs.ai>

Include the following information:
- Description of the vulnerability
- Steps to reproduce the issue
- Potential impact
- Any suggested fixes (optional)

### Response Timeline

- **Initial response**: Within 48 hours
- **Status update**: Within 5 business days
- **Fix timeline**: Depends on severity (critical issues prioritized)

### Disclosure Policy

- We will acknowledge your report within 48 hours
- We will provide regular updates on our progress
- Once a fix is released, we will publicly credit you (unless you prefer to remain anonymous)
- We request that you do not publicly disclose the vulnerability until we have released a fix

## Supported Versions

Cloud9 is currently in active development. Security updates will be provided for:

| Version | Supported          |
| ------- | ------------------ |
| main    | :white_check_mark: |
| < 1.0   | :x:                |

Once Cloud9 reaches 1.0, we will maintain security support for the latest major version.

## Security Best Practices

When deploying Cloud9:

1. **Keep dependencies updated**: Regularly run `cargo update` and monitor security advisories
2. **Use TLS**: Always enable TLS for client connections in production
3. **Limit network exposure**: Run Cloud9 behind a firewall or VPC
4. **Monitor logs**: Watch for unusual access patterns or errors
5. **Follow the principle of least privilege**: Grant minimal necessary permissions

## Security Audits

Cloud9 has not yet undergone a formal security audit. As the project matures, we plan to engage third-party security researchers for comprehensive audits.

## Contact

For general security questions or concerns: <security@dedaluslabs.ai>
