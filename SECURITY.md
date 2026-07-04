# Security Policy

## Supported versions

lightwalletd-rs is beta software under active development. Only the latest
commit on `main` and the latest tagged release are supported; there are no
backported security fixes for older versions.

## Reporting a vulnerability

Please **do not open a public GitHub issue** for security vulnerabilities.

Instead, use GitHub's private vulnerability reporting on this repository:
go to the **Security** tab and select **"Report a vulnerability"**. This
opens a private channel with the maintainers and keeps the report out of
public view until a fix is available.

You should expect an initial response within **7 days**.

## Scope

The server is designed to run behind TLS in production; running it with
`--no-tls-very-insecure` or other `*-very-insecure` flags is intended for
local development and testing only, and issues that only manifest when those
flags are used as documented are out of scope.
