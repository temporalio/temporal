# Run Cassandra v3.11 on macOS

### Install
```bash
brew install cassandra@3.11
```

For MacBook with ARM chip, you need to replace the JNA library with a recent version.
1. Locate the JNA file (`find $(brew --prefix) -name "jna-*.jar"`) and remove it.
2. Download a recent version of JNA jar file (5.8+) from [Maven](https://search.maven.org/artifact/net.java.dev.jna/jna).
3. Move the file you downloaded to the folder in step 1.

### Start
```bash
cassandra -f
```

### Post Installation
Verify Cassandra v3.11 is running and accessible:
```bash
cqlsh
```