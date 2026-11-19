#!/bin/sh
# Job "Markdown style", GitLab only: markdownlint-cli2 with .markdownlint-cli2.jsonc (ADR-0046,
# Google Markdown Style Guide). On GitHub the pinned markdownlint-cli2 action runs the same
# version with the same config. POSIX sh: the markdownlint-cli2 image (Alpine) has no bash; the
# job clears the image's entry point, and markdownlint-cli2 is on its PATH.
set -eu
cd "$(dirname "$0")/.."

markdownlint-cli2
