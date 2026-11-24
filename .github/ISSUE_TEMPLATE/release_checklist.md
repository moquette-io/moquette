## Release checklist

- [ ] If creating a new minor version, fork the version branch from `main` and push it on `moquette-io/moquette` repository.
- [ ] In the release branch update version in every place the previous patch was used.
- [ ] Verify it builds correctly, `./mvnw clean package`.
- [ ] Create a draft release in GitHub with Release notes generated from Changelog and git commits, highlighting breaking changes.
- [ ] Create `tar.gz` distribution
  - [ ] `./mvnw clean package -pl distribution -am -Dmaven.test.skip=true`. 
  - [ ] verify the artifact, maybe with some scripts in `test_tools/`.
- [ ] Add the `tar.gz` distribution to the release.
- [ ] Finalize the release to generate the tag on the branch
  - [ ] Check the tag exists, eventually manually create: `git tag -a v0.18.2 -m "Release v0.18.2"` and `git push upstream v0.18.2`.
- [ ] Update `README.md` on `main` to reference the new version.
- [ ] Forward port the Changelog on `main` branch.
- [ ] Verify with JBang script is pulled from JitPack.
