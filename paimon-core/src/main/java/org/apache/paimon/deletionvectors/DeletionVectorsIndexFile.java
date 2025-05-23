/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.deletionvectors;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.PathFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** DeletionVectors index file. */
public class DeletionVectorsIndexFile extends IndexFile {

    public static final String DELETION_VECTORS_INDEX = "DELETION_VECTORS";
    // Current version id is 1
    public static final byte VERSION_ID_V1 = 1;
    public static final byte VERSION_ID_V2 = 2;

    private final byte writeVersionID;
    private final MemorySize targetSizePerIndexFile;

    public DeletionVectorsIndexFile(
            FileIO fileIO, PathFactory pathFactory, MemorySize targetSizePerIndexFile) {
        this(fileIO, pathFactory, targetSizePerIndexFile, VERSION_ID_V1);
    }

    public DeletionVectorsIndexFile(
            FileIO fileIO,
            PathFactory pathFactory,
            MemorySize targetSizePerIndexFile,
            byte writeVersionID) {
        super(fileIO, pathFactory);
        this.targetSizePerIndexFile = targetSizePerIndexFile;
        this.writeVersionID = writeVersionID;
    }

    /**
     * Reads all deletion vectors from a specified file.
     *
     * @return A map where the key represents which file the DeletionVector belongs to, and the
     *     value is the corresponding DeletionVector object.
     * @throws UncheckedIOException If an I/O error occurs while reading from the file.
     */
    public Map<String, DeletionVector> readAllDeletionVectors(IndexFileMeta fileMeta) {
        LinkedHashMap<String, DeletionVectorMeta> deletionVectorMetas =
                fileMeta.deletionVectorMetas();
        checkNotNull(deletionVectorMetas);

        String indexFileName = fileMeta.fileName();
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        Path filePath = pathFactory.toPath(indexFileName);
        try (SeekableInputStream inputStream = fileIO.newInputStream(filePath)) {
            int version = checkVersion(inputStream);
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            for (DeletionVectorMeta deletionVectorMeta : deletionVectorMetas.values()) {
                deletionVectors.put(
                        deletionVectorMeta.dataFileName(),
                        readDeletionVector(dataInputStream, deletionVectorMeta.length(), version));
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to read deletion vectors from file: "
                            + filePath
                            + ", deletionVectorMetas: "
                            + deletionVectorMetas,
                    e);
        }
        return deletionVectors;
    }

    public Map<String, DeletionVector> readAllDeletionVectors(List<IndexFileMeta> indexFiles) {
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        indexFiles.forEach(indexFile -> deletionVectors.putAll(readAllDeletionVectors(indexFile)));
        return deletionVectors;
    }

    /** Reads deletion vectors from a list of DeletionFile which belong to a same index file. */
    public Map<String, DeletionVector> readDeletionVector(
            Map<String, DeletionFile> dataFileToDeletionFiles) {
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        if (dataFileToDeletionFiles.isEmpty()) {
            return deletionVectors;
        }

        String indexFile = dataFileToDeletionFiles.values().stream().findAny().get().path();
        try (SeekableInputStream inputStream = fileIO.newInputStream(new Path(indexFile))) {
            int version = checkVersion(inputStream);
            for (String dataFile : dataFileToDeletionFiles.keySet()) {
                DeletionFile deletionFile = dataFileToDeletionFiles.get(dataFile);
                checkArgument(deletionFile.path().equals(indexFile));
                inputStream.seek(deletionFile.offset());
                DataInputStream dataInputStream = new DataInputStream(inputStream);
                deletionVectors.put(
                        dataFile,
                        readDeletionVector(dataInputStream, (int) deletionFile.length(), version));
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to read deletion vector from file: " + indexFile, e);
        }
        return deletionVectors;
    }

    public DeletionVector readDeletionVector(DeletionFile deletionFile) {
        String indexFile = deletionFile.path();
        try (SeekableInputStream inputStream = fileIO.newInputStream(new Path(indexFile))) {
            int version = checkVersion(inputStream);
            checkArgument(deletionFile.path().equals(indexFile));
            inputStream.seek(deletionFile.offset());
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            return readDeletionVector(dataInputStream, (int) deletionFile.length(), version);
        } catch (Exception e) {
            throw new RuntimeException("Unable to read deletion vector from file: " + indexFile, e);
        }
    }

    /**
     * Write deletion vectors to a new file, the format of this file can be referenced at: <a
     * href="https://cwiki.apache.org/confluence/x/Tws4EQ">PIP-16</a>.
     *
     * @param input A map where the key represents which file the DeletionVector belongs to, and the
     *     value is the corresponding DeletionVector object.
     * @return A Pair object specifying the name of the written new file and a map where the key
     *     represents which file the DeletionVector belongs to and the value is a Pair object
     *     specifying the range (start position and size) within the file where the deletion vector
     *     data is located.
     * @throws UncheckedIOException If an I/O error occurs while writing to the file.
     */
    public List<IndexFileMeta> write(Map<String, DeletionVector> input) {
        try {
            DeletionVectorIndexFileWriter writer =
                    new DeletionVectorIndexFileWriter(
                            this.fileIO,
                            this.pathFactory,
                            this.targetSizePerIndexFile,
                            writeVersionID);
            return writer.write(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write deletion vectors.", e);
        }
    }

    private int checkVersion(InputStream in) throws IOException {
        int version = in.read();
        if (version != VERSION_ID_V1 && version != VERSION_ID_V2) {
            throw new RuntimeException(
                    "Version not match, actual version: "
                            + version
                            + ", expected version: "
                            + VERSION_ID_V1
                            + " or "
                            + VERSION_ID_V2);
        }
        return version;
    }

    private DeletionVector readDeletionVector(
            DataInputStream inputStream, int size, int readVersion) {
        if (readVersion == VERSION_ID_V1) {
            return readV1DeletionVector(inputStream, size);
        } else if (readVersion == VERSION_ID_V2) {
            return readV2DeletionVector(inputStream, size);
        } else {
            throw new RuntimeException("Unsupported DeletionVector version: " + writeVersionID);
        }
    }

    private DeletionVector readV1DeletionVector(DataInputStream inputStream, int size) {
        try {
            // check size
            int actualSize = inputStream.readInt();
            if (actualSize != size) {
                throw new RuntimeException(
                        "Size not match, actual size: " + actualSize + ", expected size: " + size);
            }

            // read DeletionVector bytes
            byte[] bytes = new byte[size];
            inputStream.readFully(bytes);

            // check checksum
            int checkSum = calculateChecksum(bytes);
            int actualCheckSum = inputStream.readInt();
            if (actualCheckSum != checkSum) {
                throw new RuntimeException(
                        "Checksum not match, actual checksum: "
                                + actualCheckSum
                                + ", expected checksum: "
                                + checkSum);
            }
            return DeletionVector.deserializeFromBytes(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read deletion vector", e);
        }
    }

    private DeletionVector readV2DeletionVector(DataInputStream inputStream, int size) {
        try {
            byte[] bytes = new byte[size];
            inputStream.readFully(bytes);

            return Bitmap64DeletionVector.deserializeFromBytes(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read deletion vector", e);
        }
    }

    public static int calculateChecksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
