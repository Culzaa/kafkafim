#!/usr/bin/env bash
# GitLab only: writes ~/.m2/settings.xml so that every Maven request goes to one repository,
# MAVEN_REPO_URL (Artifactory, Nexus or the GitLab package registry holding the bundle's
# maven-repo/), and Maven needs no `-o`. Credentials are not written into the file: it reads
# ${env.MAVEN_REPO_USER} and ${env.MAVEN_REPO_PASSWORD} when Maven runs.
#
#   MAVEN_REPO_URL        required, no trailing slash needed
#   MAVEN_REPO_USER       optional; with MAVEN_REPO_PASSWORD, sent as HTTP Basic authentication
#   MAVEN_REPO_PASSWORD   optional (masked variable)
#
# The Maven wrapper's own download goes through the same URL (MVNW_REPOURL) with the same
# credentials (MVNW_USERNAME, MVNW_PASSWORD); .gitlab-ci.yml sets those from these variables.
set -euo pipefail

url="${MAVEN_REPO_URL:?MAVEN_REPO_URL is not set: the Maven repository to resolve from (docs/DELIVERY.md, CI on GitLab)}"
url="${url%/}"
case "$url" in
  http://* | https://* | file://*) ;;
  *)
    echo "MAVEN_REPO_URL must start with http://, https:// or file://: $url" >&2
    exit 2
    ;;
esac
dir="${HOME:?}/.m2"
mkdir -p "$dir"
# shellcheck disable=SC2016  # ${env.X} is for Maven to expand, not the shell
{
  echo '<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0">'
  if [[ -n "${MAVEN_REPO_USER:-}" && -n "${MAVEN_REPO_PASSWORD:-}" ]]; then
    echo '  <servers>'
    echo '    <server>'
    echo '      <id>kafkafim</id>'
    echo '      <username>${env.MAVEN_REPO_USER}</username>'
    echo '      <password>${env.MAVEN_REPO_PASSWORD}</password>'
    echo '    </server>'
    echo '  </servers>'
  fi
  echo '  <mirrors>'
  echo '    <mirror>'
  echo '      <id>kafkafim</id>'
  echo '      <mirrorOf>*</mirrorOf>'
  # & < > in the URL (a query string) would break the XML.
  printf '      <url>%s</url>\n' "$(sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' <<<"$url")"
  echo '    </mirror>'
  echo '  </mirrors>'
  echo '</settings>'
} >"$dir/settings.xml"
echo "Maven resolves from $url (settings: $dir/settings.xml)"
