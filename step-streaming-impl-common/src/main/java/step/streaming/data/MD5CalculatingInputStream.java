package step.streaming.data;

import step.streaming.util.DigestHelper;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

@SuppressWarnings("NullableProblems") // silence IntelliJ-specific bogus warning
public class MD5CalculatingInputStream extends FilterInputStream {

    private final MessageDigest digest;
    private boolean closed = false;
    private String checksum;

    public MD5CalculatingInputStream(InputStream in) {
        super(in);
        this.digest = DigestHelper.newMD5Digest();
    }

    @Override
    public int read() throws IOException {
        int b = in.read();
        if (b != -1) {
            digest.update((byte) b);
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = in.read(b, off, len);
        if (n > 0) {
            digest.update(b, off, n);
        }
        return n;
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
