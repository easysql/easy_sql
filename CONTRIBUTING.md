# Contributing to EasySQL

- [Contributing to EasySQL](#contributing-to-easysql)
  - [Architecture Design](#architecture-design)
  - [Build and Run EasySQL](#build-and-run-EasySQL)
  - [Create Tracking Issue if Necessary](#create-tracking-issue-if-necessary)
  - [Write Tests](#write-tests)
  - [Running Test and Checks](#running-test-and-checks)
  - [Submit a PR](#submit-a-pr)
    - [Pull Request Title](#pull-request-title)
    - [Pull Request Description](#pull-request-description)
    - [Sign DCO (Developer Certificate of Origin)](#sign-dco-developer-certificate-of-origin)

Thanks for your contribution! The EasySQL project welcomes contribution of various types -- new features, bug fixes
and reports, typo fixes, etc. If you want to contribute to the EasySQL project, you will need to pass necessary
checks and sign DCO. If you have any question, feel free to ping community members on GitHub and in Slack channels.

## Architecture Design

TODO: need to enhance this part

## Build and Run EasySQL

TODO: need to enhance this part

## Create Tracking Issue if Necessary

If you are working on a large feature (>= 300 LoCs), it is recommended to create a tracking issue first, so that
contributors and maintainers can understand the issue better and discuss how to proceed and implement the features.

## Write Tests

TODO: need to enhance this part

## Running Test and Checks

We provide a simple make command to run all the checks:

```shell
make unit-test
```

After all the checks pass, your changes will likely be accepted.

## Submit a PR

### Pull Request Title

As described in [here](https://github.com/commitizen/conventional-commit-types/blob/master/index.json), a valid PR title should begin with one of the following prefixes:

- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding missing tests or correcting existing tests
- `build`: Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
- `ci`: Changes to EasySQL CI configuration files and scripts
- `chore`: Other changes that don't modify src or test files
- `revert`: Reverts a previous commit

For example, a PR title could be:

- `refactor: modify sql processor protobuf package path`
- `feat(processor): support clickhouse as backend.`


> `<type>(<scope>): <subject>`
>
> ```
> feat(scope): add hat wobble
> ^--^ ^---^   ^------------^
> |    |       |
> |    |       +-> Summary in present tense.
> |    |
> |    +---> Scope: executor, storage, etc.
> |
> +-------> Type: chore, docs, feat, fix, refactor, style, or test.
> ```


### Pull Request Description

- If your PR is small (such as a typo fix), you can go brief.
- If it is large and you have changed a lot, it's better to write more details.

### Sign DCO (Developer Certificate of Origin)

Contributors will need to sign DCO in their commits. From [GitHub App's DCO](https://github.com/apps/dco) page:

The Developer Certificate of Origin (DCO) is a lightweight way for contributors to certify that they wrote or otherwise
have the right to submit the code they are contributing to the project. Here is the full text of the DCO, reformatted
for readability:

> By making a contribution to this project, I certify that:
> 
> The contribution was created in whole or in part by me and I have the right to submit it under the open source license indicated in the file; or
> 
> The contribution is based upon previous work that, to the best of my knowledge, is covered under an appropriate open source license and I have the right under that license to submit that work with modifications, whether created in whole or in part by me, under the same open source license (unless I am permitted to submit under a different license), as indicated in the file; or
> 
> The contribution was provided directly to me by some other person who certified 1., 2. or 3. and I have not modified it.
> 
> I understand and agree that this project and the contribution are public and that a record of the contribution (including all personal information I submit with it, including my sign-off) is maintained indefinitely and may be redistributed consistent with this project or the open source license(s) involved.

Contributors will need to add a `Signed-off-by` line in all their commits:

```
Signed-off-by: Random J Developer <random@developer.example.org>
```

The `git` command provides `-s` parameter to attach DCO to the commits.

```
git commit -m "feat(scope): commit messages" -s
```
