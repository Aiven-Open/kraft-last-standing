# Welcome contributor 👋

Contributions of all sorts are welcome, whether it's raising an idea for improvements,
reporting a bug, helping in reviewing open pull requests, or generally raising an issue.

When contributing please keep this in mind:

- Use issues to report bugs, raise ideas and propose features.
- Write code consistent with the project style and make sure tests and static type
  checks are passing, and that 100% test coverage is maintained.

The [README][readme] contains instructions for getting started with development.

[readme]:
  https://github.com/Aiven-Open/kraft-last-standing/blob/main/README.md#development

We use [squash merging] to achieve a linear git history. This means every PR needs to be
a logic unit of changes to the code-base, that makes sense to be squashed. This also
allows for pushing many "fix" commits to feature branches without using force push.

[squash merging]:
  https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/about-pull-request-merges#squash-and-merge-your-commits

## Releases

Releases of the project follow [Semantic Versioning v2.0.0][semver]. The version of the
package is derived from its git tag. To publish a new release, create a release in the
GitHub UI and publish it, choosing to create a tag on publish that is identical to the
version, such as `1.2.0`, without a `v` prefix. A GitHub Actions workflow will then
publish the package to PyPI, and the documentation of the release. Unless the release is
marked as a pre-release, its documentation becomes the latest.

[semver]: https://semver.org/spec/v2.0.0.html
