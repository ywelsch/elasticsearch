/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.preallocate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.SuppressForbidden;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

public class Preallocate {

    private static final Logger logger = LogManager.getLogger(Preallocate.class);

    public static void preallocate(final Path cacheFile, final long fileSize) throws IOException {
        if (Constants.LINUX) {
            preallocate(cacheFile, fileSize, new LinuxPreallocator());
        } else if (Constants.MAC_OS_X) {
            preallocate(cacheFile, fileSize, new MacOsPreallocator());
        } else {
            preallocate(cacheFile, fileSize, new UnsupportedPreallocator());
        }
    }

    public static void punchHole(final FileChannel fileChannel, final long offset, final long length) {
        if (Constants.LINUX) {
            punchHole(fileChannel, offset, length, new LinuxPreallocator());
        } else if (Constants.MAC_OS_X) {
            punchHole(fileChannel, offset, length, new MacOsPreallocator());
        }
    }

    @SuppressForbidden(reason = "need access to fd on FileOutputStream")
    private static void preallocate(final Path cacheFile, final long fileSize, final Preallocator prealloactor) throws IOException {
        if (prealloactor.available() == false) {
            logger.warn("failed to pre-allocate cache file [{}] as native methods are not available", cacheFile);
        }
        boolean success = false;
        try (FileOutputStream fileChannel = new FileOutputStream(cacheFile.toFile())) {
            long currentSize = fileChannel.getChannel().size();
            if (currentSize < fileSize) {
                final int fd = AccessController.doPrivileged(new FileDescriptorFieldAction(fileChannel));
                final int errno = prealloactor.preallocate(fd, currentSize, fileSize - currentSize);
                if (errno == 0) {
                    success = true;
                    logger.debug("pre-allocated cache file [{}] using native methods", cacheFile);
                } else {
                    logger.warn(
                        "failed to pre-allocate cache file [{}] using native methods, errno: [{}], error: [{}]",
                        cacheFile,
                        errno,
                        prealloactor.error(errno)
                    );
                }
            }
        } catch (final Exception e) {
            logger.warn(new ParameterizedMessage("failed to pre-allocate cache file [{}] using native methods", cacheFile), e);
        } finally {
            if (success == false) {
                // if anything goes wrong, delete the potentially created file to not waste disk space
                Files.deleteIfExists(cacheFile);
            }
        }
    }

    @SuppressForbidden(reason = "need access to fd on FileOutputStream")
    private static void punchHole(final FileChannel fileChannel, long offset, long length, final Preallocator prealloactor) {
        if (prealloactor.available() == false) {
            logger.warn("failed to punch hole in cache file [{}] as native methods are not available", fileChannel);
        }

        try {
            final int fd = AccessController.doPrivileged(new FileDescriptorFieldAction(fileChannel));
            final int errno = prealloactor.punch_hole(fd, offset, length);
            if (errno == 0) {
                logger.debug("punched hole in cache file [{}] using native methods", fileChannel);
            } else {
                logger.warn(
                    "failed to punch hole in cache file [{}] using native methods, errno: [{}], error: [{}]",
                    fileChannel,
                    errno,
                    prealloactor.error(errno)
                );
                throw new IOException("failed to punch hole in cache file using native methods, errno: [" + errno +
                    "], error: [" + prealloactor.error(errno) + "])");
            }
        } catch (final Exception e) {
            logger.warn(new ParameterizedMessage("failed to punch hole in cache file [{}] using native methods", fileChannel), e);
            throw new RuntimeException(e);
        }
    }

    @SuppressForbidden(reason = "need access to fd on FileOutputStream")
    private static class FileDescriptorFieldAction implements PrivilegedExceptionAction<Integer> {

        private final Object object;

        private FileDescriptorFieldAction(Object object) {
            this.object = object;
        }

        @Override
        public Integer run() throws IOException, NoSuchFieldException, IllegalAccessException {
            // accessDeclaredMembers
            final Object objectToUse = unwrapFilterChannel(object);
            final Field f;
            try {
                f = objectToUse.getClass().getDeclaredField("fd");
            } catch (NoSuchFieldException e) {
                throw new IOException("fd field not found on " + objectToUse.getClass());
            }
            // suppressAccessChecks
            f.setAccessible(true);
            final FileDescriptor fileDescriptor = (FileDescriptor) f.get(objectToUse);
            // accessDeclaredMembers
            final Field fn = fileDescriptor.getClass().getDeclaredField("fd");
            // suppressAccessChecks
            fn.setAccessible(true);
            return (Integer) fn.get(fileDescriptor);
        }

        private static Object unwrapFilterChannel(Object o) throws NoSuchFieldException, IllegalAccessException {
            try {
                Class<?> filterFileChannel = Class.forName("org.apache.lucene.mockfile.FilterFileChannel");
                final Field delegate = filterFileChannel.getDeclaredField("delegate");
                delegate.setAccessible(true);
                while (filterFileChannel.isInstance(o)) {
                    o = delegate.get(o);
                }
            } catch (ClassNotFoundException e) {
                // ignore
            }
            return o;
        }
    }

}
