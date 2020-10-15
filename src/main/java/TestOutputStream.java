import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class TestOutputStream extends ByteArrayOutputStream {

   public InputStream toGZIPInputStream() throws IOException {
       return new GZIPInputStream(new ByteArrayInputStream(buf));
   }

}
