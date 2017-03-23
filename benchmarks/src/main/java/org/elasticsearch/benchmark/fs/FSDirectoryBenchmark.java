package org.elasticsearch.benchmark.fs;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.SecureSM;
import org.elasticsearch.bootstrap.ESPolicy;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.common.io.PathUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.FilePermission;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.AccessMode;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.security.Permissions;
import java.security.Policy;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

// /Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/bin/java
// /Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home/bin/java
// /Library/Java/JavaVirtualMachines/jdk-9.jdk/Contents/Home/bin/java
@Fork(value = 1, jvm = "/Library/Java/JavaVirtualMachines/jdk-9.jdk/Contents/Home/bin/java", jvmArgs = "-Djdk.io.permissionsUseCanonicalPath=true")
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") //invoked by benchmarking framework
public class FSDirectoryBenchmark {

    @Setup
    public void setUp() throws Exception {
        // enable security manager
        Permissions policy = new Permissions();
        addPath(policy, "java.io.tmpdir", PathUtils.get(System.getProperty("java.io.tmpdir")), "read,readlink,write,delete");
        for (int i = 0; i < 100; i++) {
            addClasspathPermissions(policy); // not required in this test but ES does it
        }
        Policy.setPolicy(new ESPolicy(policy, new HashMap<>(), true));
        System.setSecurityManager(new SecureSM(new String[] { "org.elasticsearch.bootstrap.", "org.elasticsearch.cli" }));
    }

    @Benchmark
    public void measureFSOperations() throws IOException {
        for (int i = 0; i < 100; i++) {
            try (Directory dir = new NIOFSDirectory(Files.createTempDirectory("test"))) {
                try (IndexOutput indexOutput = dir.createOutput("test-file", IOContext.DEFAULT)) {
                    indexOutput.writeString("foo");
                }
                try (IndexInput indexInput = dir.openInput("test-file", IOContext.DEFAULT)) {
                    indexInput.readString();
                }
            }
        }
    }

    public static void main(String... args) throws Exception {
        FSDirectoryBenchmark benchmark = new FSDirectoryBenchmark();
        benchmark.setUp();
        benchmark.measureFSOperations();
    }

    static void addPath(Permissions policy, String configurationName, Path path, String permissions) throws IOException {
        // paths may not exist yet, this also checks accessibility
        try {
            ensureDirectoryExists(path);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to access '" + configurationName + "' (" + path + ")", e);
        }

        // add each path twice: once for itself, again for files underneath it
        policy.add(new FilePermission(path.toString(), permissions));
        policy.add(new FilePermission(path.toString() + path.getFileSystem().getSeparator() + "-", permissions));
        Path realPath = path.toRealPath();
        if (path.toString().equals(realPath.toString()) == false) {
            // Java 9 requires this
            policy.add(new FilePermission(realPath.toString(), permissions));
            policy.add(new FilePermission(realPath.toString() + realPath.getFileSystem().getSeparator() + "-", permissions));
        }
    }

    static void ensureDirectoryExists(Path path) throws IOException {
        // this isn't atomic, but neither is createDirectories.
        if (Files.isDirectory(path)) {
            // verify access, following links (throws exception if something is wrong)
            // we only check READ as a sanity test
            path.getFileSystem().provider().checkAccess(path.toRealPath(), AccessMode.READ);
        } else {
            // doesn't exist, or not a directory
            try {
                Files.createDirectories(path);
            } catch (FileAlreadyExistsException e) {
                // convert optional specific exception so the context is clear
                IOException e2 = new NotDirectoryException(path.toString());
                e2.addSuppressed(e);
                throw e2;
            }
        }
    }

    static void addClasspathPermissions(Permissions policy) throws IOException {
        // add permissions to everything in classpath
        // really it should be covered by lib/, but there could be e.g. agents or similar configured)
        for (URL url : JarHell.parseClassPath()) {
            Path path;
            try {
                path = PathUtils.get(url.toURI());
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            // resource itself
            if (Files.isDirectory(path)) {
                addPath(policy, "class.path", path, "read,readlink");
            } else {
                addSingleFilePath(policy, path, "read,readlink");
            }
        }
    }

    static void addSingleFilePath(Permissions policy, Path path, String permissions) throws IOException {
        policy.add(new FilePermission(path.toString(), permissions));
        Path realPath = path.toRealPath();
        if (path.toString().equals(realPath.toString()) == false) {
            // Java 9 requires this
            policy.add(new FilePermission(realPath.toString() + realPath.getFileSystem().getSeparator() + "-", permissions));
        }
    }
}
