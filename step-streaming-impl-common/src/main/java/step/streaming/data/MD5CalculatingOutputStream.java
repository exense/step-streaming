package step.streaming.data;

import step.streaming.data.util.DigestHelper;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;

@SuppressWarnings("NullableProblems") // silence IntelliJ-specific bogus warning
public class MD5CalculatingOutputStream extends FilterOutputStream {

    private final MessageDigest digest;
    private String checksum;
    private boolean closed = false;

    public MD5CalculatingOutputStream(OutputStream out) {
        super(out);
        this.digest = DigestHelper.newMD5Digest();
    }


    @Override
    public void write(int b) throws IOException {
        digest.update((byte) b);
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        digest.update(b, off, len);
        out.write(b, off, len);
    }

    public String getChecksum() {
        if (!closed) {
            throw new IllegalStateException("Stream must be closed before requesting checksum");
        }
        if (checksum == null) {
            checksum = DigestHelper.toHexString(digest.digest());
        }
        return checksum;
    }


    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            super.close();
        }
    }
}
