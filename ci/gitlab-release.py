#!/usr/bin/env python3
"""Creates a GitLab release with its files, with the job token only (no release-cli).

usage: gitlab-release.py TAG IMAGE_REF CHART_REF FILE...

Each FILE goes to the generic package registry (package "kafkafim", version TAG without "v")
and becomes a release asset link. A pre-release tag (a "-" in the version) is noted in the
description. Environment: CI_API_V4_URL, CI_PROJECT_ID, CI_JOB_TOKEN, CI_COMMIT_SHA.
"""
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


def request(method, url, token, body=None, content_type=None):
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("JOB-TOKEN", token)
    if content_type:
        req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=300) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        sys.exit(f"{method} {url}: HTTP {error.code}: {error.read().decode(errors='replace')[:500]}")


def main(argv):
    if len(argv) < 5:
        sys.exit(__doc__)
    tag, image_ref, chart_ref, files = argv[0], argv[1], argv[2], argv[3:]
    api = os.environ["CI_API_V4_URL"].rstrip("/")
    project = os.environ["CI_PROJECT_ID"]
    token = os.environ["CI_JOB_TOKEN"]
    version = tag[1:] if tag.startswith("v") else tag
    base = f"{api}/projects/{project}/packages/generic/kafkafim/{urllib.parse.quote(version)}"
    links = []
    for path in files:
        name = os.path.basename(path)
        with open(path, "rb") as handle:
            request("PUT", f"{base}/{urllib.parse.quote(name)}", token, handle.read(), "application/octet-stream")
        print(f"uploaded {name}")
        links.append({"name": name, "url": f"{base}/{urllib.parse.quote(name)}", "link_type": "other"})
    pre = "-" in version
    description = "\n".join(
        [
            f"kafkafim {version}" + (" (pre-release)" if pre else ""),
            "",
            f"- Image: `{image_ref}`",
            f"- Chart: `oci://{chart_ref}`",
            f"- Commit: {os.environ.get('CI_COMMIT_SHA', '')}",
        ]
    )
    body = json.dumps({"tag_name": tag, "name": tag, "description": description, "assets": {"links": links}})
    request("POST", f"{api}/projects/{project}/releases", token, body.encode(), "application/json")
    print(f"created the release {tag} with {len(links)} assets")


main(sys.argv[1:])
