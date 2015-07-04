package org.apache.lucene.store;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a Directory Wrapper that adds methods
 * intended to be used only by unit tests.
 * It also adds a number of features useful for testing:
 * <ul>
 * <li> Instances created are tracked
 * to ensure they are closed by the test.
 * <li> When a MockDirectoryWrapper is closed, it will throw an exception if
 * it has any open files against it (with a stacktrace indicating where
 * they were opened from).
 * <li> When a MockDirectoryWrapper is closed, it runs CheckIndex to test if
 * the index was corrupted.
 * <li> MockDirectoryWrapper simulates some "features" of Windows, such as
 * refusing to write/delete to open files.
 * </ul>
 */
public class MockDirectoryWrapper extends FilterDirectory {

    final AtomicInteger inputCloneCount = new AtomicInteger();
    protected volatile boolean isOpen = true;
    long maxSize;
    long maxUsedSize;
    double randomIOExceptionRate;
    double randomIOExceptionRateOnOpen;
    boolean noDeleteOpenFile = true;
    boolean assertNoDeleteOpenFile = false;
    boolean preventDoubleWrite = true;
    boolean trackDiskUsage = false;
    boolean useSlowOpenClosers = false;
    boolean allowRandomFileNotFoundException = true;
    boolean allowReadingFilesStillOpenForWrite = false;
    ConcurrentMap<String, RuntimeException> openLocks = new ConcurrentHashMap<>();
    volatile boolean crashed;
    boolean verboseClone;
    ArrayList<Failure> failures;
    private Set<String> unSyncedFiles;
    private Set<String> createdFiles;
    private Set<String> openFilesForWrite = new HashSet<>();
    private Map<Closeable, Exception> openFileHandles = Collections.synchronizedMap(new IdentityHashMap<Closeable, Exception>());
    private Map<String, Integer> openFiles;
    private Set<String> openFilesDeleted;
    private Set<String> triedToDelete;
    private boolean failOnCreateOutput = true;
    private boolean failOnOpenInput = true;

    public MockDirectoryWrapper(Directory delegate) {
        super(delegate);
        init();
    }

    private synchronized void init() {
        if (openFiles == null) {
            openFiles = new HashMap<>();
            openFilesDeleted = new HashSet<>();
            triedToDelete = new HashSet<>();
        }

        if (createdFiles == null) {
            createdFiles = new HashSet<>();
        }
        if (unSyncedFiles == null) {
            unSyncedFiles = new HashSet<>();
        }
    }

    public int getInputCloneCount() {
        return inputCloneCount.get();
    }

    /**
     * If set to true, we print a fake exception
     * with filename and stacktrace on every indexinput clone()
     */
    public void setVerboseClone(boolean v) {
        verboseClone = v;
    }

    public void setTrackDiskUsage(boolean v) {
        trackDiskUsage = v;
    }

    /**
     * If set to true, we throw an IOException if the same
     * file is opened by createOutput, ever.
     */
    public void setPreventDoubleWrite(boolean value) {
        preventDoubleWrite = value;
    }

    /**
     * If set to true (the default), when we throw random
     * IOException on openInput or createOutput, we may
     * sometimes throw FileNotFoundException or
     * NoSuchFileException.
     */
    public void setAllowRandomFileNotFoundException(boolean value) {
        allowRandomFileNotFoundException = value;
    }

    /**
     * If set to true, you can open an inputstream on a file
     * that is still open for writes.
     */
    public void setAllowReadingFilesStillOpenForWrite(boolean value) {
        allowReadingFilesStillOpenForWrite = value;
    }

    /**
     * Add a rare small sleep to catch race conditions in open/close
     * You can enable this if you need it.
     */
    public void setUseSlowOpenClosers(boolean v) {
        useSlowOpenClosers = v;
    }

    @Override
    public synchronized void sync(Collection<String> names) throws IOException {
        maybeThrowDeterministicException();
        if (crashed) {
            throw new IOException("cannot sync after crash");
        }
        for (String name : names) {
            in.sync(Collections.singleton(name));
            unSyncedFiles.remove(name);
        }
    }

    @Override
    public synchronized void renameFile(String source, String dest) throws IOException {
        maybeThrowDeterministicException();
        if (crashed) {
            throw new IOException("cannot rename after crash");
        }
        if (openFiles.containsKey(source)) {
            if (assertNoDeleteOpenFile) {
                throw (AssertionError) fillOpenTrace(new AssertionError("MockDirectoryWrapper: file \"" + source + "\" is still open: cannot rename"), source, true);
            } else if (noDeleteOpenFile) {
                throw (IOException) fillOpenTrace(new IOException("MockDirectoryWrapper: file \"" + source + "\" is still open: cannot rename"), source, true);
            }
        }
        boolean success = false;
        try {
            in.renameFile(source, dest);
            success = true;
        } finally {
            if (success) {
                if (unSyncedFiles.contains(source)) {
                    unSyncedFiles.remove(source);
                    unSyncedFiles.add(dest);
                }
                openFilesDeleted.remove(source);
            }
        }
    }

    public synchronized final long sizeInBytes() throws IOException {
        if (in instanceof RAMDirectory) {
            return ((RAMDirectory) in).ramBytesUsed();
        } else {
            long size = 0;
            for (String file : in.listAll()) {
                if (!file.startsWith("extra")) {
                    size += in.fileLength(file);
                }
            }
            return size;
        }
    }

    public long getMaxSizeInBytes() {
        return this.maxSize;
    }

    public void setMaxSizeInBytes(long maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * Returns the peek actual storage used (bytes) in this
     * directory.
     */
    public long getMaxUsedSizeInBytes() {
        return this.maxUsedSize;
    }

    public void resetMaxUsedSizeInBytes() throws IOException {
        this.maxUsedSize = getRecomputedActualSizeInBytes();
    }

    public boolean getNoDeleteOpenFile() {
        return noDeleteOpenFile;
    }

    /**
     * Emulate windows whereby deleting an open file is not
     * allowed (raise IOException).
     */
    public void setNoDeleteOpenFile(boolean value) {
        this.noDeleteOpenFile = value;
    }

    public boolean getAssertNoDeleteOpenFile() {
        return assertNoDeleteOpenFile;
    }

    /**
     * Trip a test assert if there is an attempt
     * to delete an open file.
     */
    public void setAssertNoDeleteOpenFile(boolean value) {
        this.assertNoDeleteOpenFile = value;
    }

    public double getRandomIOExceptionRate() {
        return randomIOExceptionRate;
    }

    /**
     * If 0.0, no exceptions will be thrown.  Else this should
     * be a double 0.0 - 1.0.  We will randomly throw an
     * IOException on the first write to an OutputStream based
     * on this probability.
     */
    public void setRandomIOExceptionRate(double rate) {
        randomIOExceptionRate = rate;
    }

    public double getRandomIOExceptionRateOnOpen() {
        return randomIOExceptionRateOnOpen;
    }

    /**
     * If 0.0, no exceptions will be thrown during openInput
     * and createOutput.  Else this should
     * be a double 0.0 - 1.0 and we will randomly throw an
     * IOException in openInput and createOutput with
     * this probability.
     */
    public void setRandomIOExceptionRateOnOpen(double rate) {
        randomIOExceptionRateOnOpen = rate;
    }

    /**
     * returns current open file handle count
     */
    public synchronized long getFileHandleCount() {
        return openFileHandles.size();
    }

    @Override
    public synchronized void deleteFile(String name) throws IOException {
        deleteFile(name, false);
    }

    // sets the cause of the incoming ioe to be the stack
    // trace when the offending file name was opened
    private synchronized Throwable fillOpenTrace(Throwable t, String name, boolean input) {
        for (Map.Entry<Closeable, Exception> ent : openFileHandles.entrySet()) {
            if (input && ent.getKey() instanceof MockIndexInputWrapper && ((MockIndexInputWrapper) ent.getKey()).name.equals(name)) {
                t.initCause(ent.getValue());
                break;
            } else if (!input && ent.getKey() instanceof MockIndexOutputWrapper && ((MockIndexOutputWrapper) ent.getKey()).name.equals(name)) {
                t.initCause(ent.getValue());
                break;
            }
        }
        return t;
    }

    private synchronized void deleteFile(String name, boolean forced) throws IOException {

        maybeThrowDeterministicException();

        if (crashed && !forced) {
            throw new IOException("cannot delete after crash");
        }

        if (unSyncedFiles.contains(name)) {
            unSyncedFiles.remove(name);
        }
        if (!forced && (noDeleteOpenFile || assertNoDeleteOpenFile)) {
            if (openFiles.containsKey(name)) {
                openFilesDeleted.add(name);

                if (!assertNoDeleteOpenFile) {
                    throw (IOException) fillOpenTrace(new IOException("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot delete"), name, true);
                } else {
                    throw (AssertionError) fillOpenTrace(new AssertionError("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot delete"), name, true);
                }
            } else {
                openFilesDeleted.remove(name);
            }
        }
        triedToDelete.remove(name);
        in.deleteFile(name);
    }

    /**
     * Returns true if {@link #deleteFile} was called with this
     * fileName, but the virus checker prevented the deletion.
     */
    public boolean didTryToDelete(String fileName) {
        return triedToDelete.contains(fileName);
    }

    public synchronized Set<String> getOpenDeletedFiles() {
        return new HashSet<>(openFilesDeleted);
    }

    public void setFailOnCreateOutput(boolean v) {
        failOnCreateOutput = v;
    }

    @Override
    public synchronized IndexOutput createOutput(String name, IOContext context) throws IOException {
        maybeThrowDeterministicException();
        if (failOnCreateOutput) {
            maybeThrowDeterministicException();
        }
        if (crashed) {
            throw new IOException("cannot createOutput after crash");
        }
        init();
        synchronized (this) {
            if (preventDoubleWrite && createdFiles.contains(name) && !name.equals("segments.gen")) {
                throw new IOException("file \"" + name + "\" was already written to");
            }
        }
        if ((noDeleteOpenFile || assertNoDeleteOpenFile) && openFiles.containsKey(name)) {
            if (!assertNoDeleteOpenFile) {
                throw new IOException("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot overwrite");
            } else {
                throw new AssertionError("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot overwrite");
            }
        }
        if (crashed) {
            throw new IOException("cannot createOutput after crash");
        }
        unSyncedFiles.add(name);
        createdFiles.add(name);
        if (in instanceof RAMDirectory) {
            RAMDirectory ramdir = (RAMDirectory) in;
            RAMFile file = new RAMFile(ramdir);
            RAMFile existing = ramdir.fileMap.get(name);
            if (existing != null && !name.equals("segments.gen") && preventDoubleWrite) {
                throw new IOException("file " + name + " already exists");
            } else {
                if (existing != null) {
                    ramdir.sizeInBytes.getAndAdd(-existing.sizeInBytes);
                    existing.directory = null;
                }
                ramdir.fileMap.put(name, file);
            }
        }
        IndexOutput delegateOutput = in.createOutput(name, context);
        final IndexOutput io = new MockIndexOutputWrapper(this, delegateOutput, name);
        addFileHandle(io, name, Handle.Output);
        openFilesForWrite.add(name);
        return io;
    }

    synchronized void addFileHandle(Closeable c, String name, Handle handle) {
        Integer v = openFiles.get(name);
        if (v != null) {
            v = v + 1;
            openFiles.put(name, v);
        } else {
            openFiles.put(name, 1);
        }
        openFileHandles.put(c, new RuntimeException("unclosed Index" + handle.name() + ": " + name));
    }

    public void setFailOnOpenInput(boolean v) {
        failOnOpenInput = v;
    }

    @Override
    public synchronized IndexInput openInput(String name, IOContext context) throws IOException {
        maybeThrowDeterministicException();
        if (failOnOpenInput) {
            maybeThrowDeterministicException();
        }
        if (!allowReadingFilesStillOpenForWrite && openFilesForWrite.contains(name) && !name.startsWith("segments")) {
            throw (IOException) fillOpenTrace(new IOException("MockDirectoryWrapper: file \"" + name + "\" is still open for writing"), name, false);
        }
        IndexInput delegateInput = in.openInput(name, context);
        final IndexInput ii = new MockIndexInputWrapper(this, name, delegateInput);
        addFileHandle(ii, name, Handle.Input);
        return ii;
    }

    /**
     * Provided for testing purposes.  Use sizeInBytes() instead.
     */
    public synchronized final long getRecomputedSizeInBytes() throws IOException {
        if (!(in instanceof RAMDirectory)) {
            return sizeInBytes();
        }
        long size = 0;
        for (final RAMFile file : ((RAMDirectory) in).fileMap.values()) {
            size += file.ramBytesUsed();
        }
        return size;
    }

    /**
     * Like getRecomputedSizeInBytes(), but, uses actual file
     * lengths rather than buffer allocations (which are
     * quantized up to nearest
     * RAMOutputStream.BUFFER_SIZE (now 1024) bytes.
     */
    public final synchronized long getRecomputedActualSizeInBytes() throws IOException {
        if (!(in instanceof RAMDirectory)) {
            return sizeInBytes();
        }
        long size = 0;
        for (final RAMFile file : ((RAMDirectory) in).fileMap.values()) {
            size += file.length;
        }
        return size;
    }

    @Override
    public synchronized void close() throws IOException {
        if (isOpen) {
            isOpen = false;
        } else {
            in.close(); // but call it again on our wrapped dir
            return;
        }
        boolean success = false;
        try {
            Set<String> pendingDeletions = new HashSet<>(openFilesDeleted);
            pendingDeletions.addAll(triedToDelete);
            if (openFiles == null) {
                openFiles = new HashMap<>();
                openFilesDeleted = new HashSet<>();
            }
            if (openFiles.size() > 0) {
                // print the first one as it's very verbose otherwise
                Exception cause = null;
                Iterator<Exception> stacktraces = openFileHandles.values().iterator();
                if (stacktraces.hasNext()) {
                    cause = stacktraces.next();
                }
                // RuntimeException instead of IOException because
                // super() does not throw IOException currently:
                throw new RuntimeException("MockDirectoryWrapper: cannot close: there are still open files: " + openFiles, cause);
            }
            if (openLocks.size() > 0) {
                Exception cause = null;
                Iterator<RuntimeException> stacktraces = openLocks.values().iterator();
                if (stacktraces.hasNext()) {
                    cause = stacktraces.next();
                }
                throw new RuntimeException("MockDirectoryWrapper: cannot close: there are still open locks: " + openLocks, cause);
            }

            success = true;
        } finally {
            if (success) {
                IOUtils.close(in);
            } else {
                IOUtils.closeWhileHandlingException(in);
            }
        }
    }

    synchronized void removeOpenFile(Closeable c, String name) {
        Integer v = openFiles.get(name);
        // Could be null when crash() was called
        if (v != null) {
            if (v.intValue() == 1) {
                openFiles.remove(name);
            } else {
                v = Integer.valueOf(v.intValue() - 1);
                openFiles.put(name, v);
            }
        }

        openFileHandles.remove(c);
    }

    public synchronized void removeIndexOutput(IndexOutput out, String name) {
        openFilesForWrite.remove(name);
        removeOpenFile(out, name);
    }

    public synchronized void removeIndexInput(IndexInput in, String name) {
        removeOpenFile(in, name);
    }

    /**
     * add a Failure object to the list of objects to be evaluated
     * at every potential failure point
     */
    synchronized public void failOn(Failure fail) {
        if (failures == null) {
            failures = new ArrayList<>();
        }
        failures.add(fail);
    }

    /**
     * Iterate through the failures list, giving each object a
     * chance to throw an IOE
     */
    synchronized void maybeThrowDeterministicException() throws IOException {
        if (failures != null) {
            for (int i = 0; i < failures.size(); i++) {
                try {
                    failures.get(i).eval(this);
                } catch (Throwable t) {
                    IOUtils.reThrow(t);
                }
            }
        }
    }

    @Override
    public synchronized String[] listAll() throws IOException {
        return in.listAll();
    }

    @Override
    public synchronized long fileLength(String name) throws IOException {
        return in.fileLength(name);
    }

    @Override
    public String toString() {
        if (maxSize != 0) {
            return "MockDirectoryWrapper(" + in + ", current=" + maxUsedSize + ",max=" + maxSize + ")";
        } else {
            return super.toString();
        }
    }

    @Override
    public final ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
        return super.openChecksumInput(name, context);
    }

    @Override
    public final void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        super.copyFrom(from, src, dest, context);
    }

    @Override
    protected final void ensureOpen() throws AlreadyClosedException {
        super.ensureOpen();
    }

    private enum Handle {
        Input, Output
    }

    public static class Failure {
        protected boolean doFail;

        public void eval(MockDirectoryWrapper dir) throws IOException {
        }

        public Failure reset() {
            return this;
        }

        public void setDoFail() {
            doFail = true;
        }

        public void clearDoFail() {
            doFail = false;
        }
    }

    /**
     * Use this when throwing fake {@code IOException},
     * e.g. from {@link MockDirectoryWrapper.Failure}.
     */
    public static class FakeIOException extends IOException {
    }
}